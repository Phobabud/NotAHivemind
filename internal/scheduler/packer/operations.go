package packer

import (
	"ClusterManager/internal/models"
	"ClusterManager/internal/scheduler/core"
	"ClusterManager/internal/scheduler/service/coordinate"
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
)

func (p *Packer) processPendingJobs(ctx context.Context) {
	pendingJobs := p.state.PendingJobs()
	for _, id := range pendingJobs {
		job, err := p.state.Job(id)
		if err != nil { // Removed from queue
			continue
		}

		foundLocalCluster := false

		for _, cluster := range p.state.LocalClusters() {
			if time.Since(cluster.LastUpdated) > 5*time.Second {
				continue
			}

			if job.CPURequirement > cluster.MaxCpu-cluster.UsedCpu || job.MemoryRequirement > cluster.MaxMem-cluster.UsedMem {
				continue
			}

			glog.V(3).Infof("Found local cluster to pack into: %s", cluster.ID)

			if err := p.state.AssignJob(job.Id, cluster.ID); err != nil {
				glog.Errorf("Assign job %s failed: %v", job.Id, err)
				continue
			}
			if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
				p.state.UnassignJob(job.Id)
				if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
					glog.Warningf("Rollback job %s failed: %v", job.Id, err)
				}
				glog.Warningf("Commit job %s failed: %v", job.Id, err)
				continue
			}
			if exists, err := p.sc.Cluster(cluster.ID).DispatchJob(ctx, job); err != nil || !exists {
				p.state.UnassignJob(job.Id)
				if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
					glog.Warningf("Rollback job %s failed: %v", job.Id, err)
				}
				glog.Warningf("Dispatch job %s failed. Rolled back job.", job.Id)
				continue
			}
			glog.V(2).Infof("Job %s assigned to local cluster %s", job.Id, id)
			foundLocalCluster = true
			break
		}
		if !foundLocalCluster {
			if err := p.Redistribute(ctx, job); err != nil {
				glog.V(2).Infof("Redistribute job %s failed: %v", job.Id, err)
			}
		}
	}
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
			return nil
		}); err != nil {
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to redistribute job %s to any peer", job.Id)
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
