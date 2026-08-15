package packer

import (
	"NotAHiveMind/internal/models"
	"NotAHiveMind/internal/scheduler/core"
	"NotAHiveMind/internal/scheduler/service/coordinate"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/glog"
)

func (p *Packer) processPendingJobs(ctx context.Context, limit int) {
	jobs := p.fetchAndSortPendingJobs()
	if len(jobs) == 0 {
		return
	}

	clusters := p.state.LocalClusters()
	numClusters := len(clusters)
	redistributed := 0
	redistributeQuota := p.calculateRedistributionQuota(len(jobs))

	for i, jobId := range jobs {
		job, err := p.state.Job(jobId)
		if err != nil {
			continue
		}

		packedLocally := false

		if numClusters <= 0 {
			continue
		}

		for j := 0; j < numClusters; j++ {
			targetIdx := (i + j) % numClusters
			cluster := clusters[targetIdx]

			if cluster.MaxCpu-cluster.UsedCpu >= job.CPURequirement && cluster.MaxMem-cluster.UsedMem >= job.MemoryRequirement {
				p.state.SoftUpdateCluster(cluster.ID, cluster.UsedCpu+job.CPURequirement, cluster.UsedMem+job.MemoryRequirement)

				cluster.UsedCpu += job.CPURequirement
				cluster.UsedMem += job.MemoryRequirement

				go p.attemptJobAssignment(ctx, cluster.ID, job)
				packedLocally = true
				break
			}
		}

		if !packedLocally && redistributed < redistributeQuota {
			redistributed++
			go func(j *core.Job) {
				if err := p.Redistribute(ctx, j); err != nil {
					glog.V(2).Infof("Redistribute job %s failed: %v", j.Id, err)
				}
			}(job)
		}
	}
}

func (p *Packer) fetchAndSortPendingJobs() []string {
	pendingIDs := p.state.PendingJobs()
	if len(pendingIDs) == 0 {
		return nil
	}

	var jobs []*core.Job
	for _, id := range pendingIDs {
		if job, err := p.state.Job(id); err == nil {
			jobs = append(jobs, job)
		}
	}

	// Sort jobs: Priority (Highest First), then Memory (Largest First)
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority > jobs[j].Priority
		}
		return jobs[i].MemoryRequirement > jobs[j].MemoryRequirement
	})

	var sortedIDs []string
	for _, j := range jobs {
		sortedIDs = append(sortedIDs, j.Id)
	}

	return sortedIDs
}

func (p *Packer) calculateRedistributionQuota(jobCount int) int {
	quota := int(float64(jobCount) * 0.25)
	if quota < 5 {
		return 5
	}
	return quota
}

func (p *Packer) attemptJobAssignment(ctx context.Context, clusterId string, job *core.Job) bool {
	if err := p.state.AssignJob(job.Id, clusterId); err != nil {
		glog.Errorf("Assign job %s failed: %v", job.Id, err)
		return false
	}
	if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
		glog.Warningf("Commit job %s failed: %v", job.Id, err)
		if errors.Is(err, core.ErrCASConcurrentConflict) {
			p.state.PurgeJob(job.Id)
			glog.V(2).Infof("Job %s purged due to concurrent CAS conflict with peer", job.Id)
			return false
		}

		p.state.UnassignJob(job.Id)
		if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
			glog.Warningf("Rollback job %s failed: %v", job.Id, err)
		}
		return false
	}
	if exists, err := p.sc.Cluster(clusterId).DispatchJob(ctx, job); err != nil || !exists {
		p.state.UnassignJob(job.Id)
		if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
			glog.Warningf("Rollback job %s failed: %v", job.Id, err)
		}
		glog.Warningf("Dispatch job %s failed. Rolled back job.", job.Id)
		return false
	}
	glog.V(2).Infof("Job %s assigned to local cluster %s", job.Id, clusterId)
	return true
}

func (p *Packer) Redistribute(ctx context.Context, job *core.Job) error {
	limitedJobScope := &core.Job{
		Id:                job.Id,
		ImageAlias:        job.ImageAlias,
		Status:            models.Pending,
		CPURequirement:    job.CPURequirement,
		MemoryRequirement: job.MemoryRequirement,
		Priority:          job.Priority,
	}
	peers := p.state.TopFreePeers(5, 5*time.Second, job.CPURequirement, job.MemoryRequirement)
	if len(peers) == 0 {
		return nil
	}

	for _, bestFitId := range peers {
		schedulerState := p.state.PeerSchedulerState(bestFitId)
		fit := false
		for _, schedulerCluster := range schedulerState.ClusterStates {
			if schedulerCluster.UsedCpu+job.CPURequirement <= schedulerCluster.MaxCpu && schedulerCluster.UsedMem+job.MemoryRequirement <= schedulerCluster.MaxMem {
				fit = true
				break
			}
		}
		if !fit {
			continue
		}

		if err := p.sc.CoordinateHandler.OpConn(ctx, bestFitId, func(ct context.Context, co *coordinate.Conn) error {
			ok, _, err := co.Redistribute(ct, p.state.Id(), limitedJobScope, true)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("failed to redistribute")
			}
			return nil
		}); err != nil {
			continue
		}

		if err := p.sc.CoordinateHandler.OpConn(ctx, bestFitId, func(ct context.Context, co *coordinate.Conn) error {
			ok, _, err := co.Redistribute(ct, p.state.Id(), job, false)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("failed to redistribute")
			}
			p.state.PurgeJob(job.Id)
			return nil
		}); err != nil {
			continue
		}
		return nil
	}

	return nil
}

func (p *Packer) SendHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			glog.V(2).Info("Heartbeat loop shutting down.")
			return
		case <-ticker.C:
			p.sc.CoordinateHandler.ForEach(ctx, func(ct context.Context, co *coordinate.Conn) error {
				if err := co.SendHeartbeat(ct); err != nil {
					return err
				}
				return nil
			})
		}
	}
}
