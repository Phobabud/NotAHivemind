package packer

import (
	"NotAHiveMind/internal/scheduler/core"
	"NotAHiveMind/internal/scheduler/service/coordinate"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

// processPendingJobs is the main coordinator. It securely pops jobs from the
// State's generic queue in O(1) time and packs them using a round-robin placement algorithm.
func (p *Packer) processPendingJobs(ctx context.Context, limit int) {
	pendingCount := len(p.state.PendingJobs())
	if pendingCount == 0 {
		return
	}

	clusters := p.state.LocalClusters()
	numClusters := len(clusters)

	var redistributed int32
	redistributeQuota := int32(p.calculateRedistributionQuota(limit))

	// Bounded Worker Pool: Prevents the scheduler from spiraling out of control
	maxWorkers := 20
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	// Calculate the absolute ceilings for local capacity to safely query the Queue constraints
	var maxCPU, maxMem int64
	for _, c := range clusters {
		if avail := c.MaxCpu - c.UsedCpu; avail > maxCPU {
			maxCPU = avail
		}
		if avail := c.MaxMem - c.UsedMem; avail > maxMem {
			maxMem = avail
		}
	}

	for i := 0; i < limit; i++ {
		searchCPU, searchMem := maxCPU, maxMem

		// If we are allowed to shed load, tell the queue to ignore local capacity constraints
		// so we can redistribute jobs that are larger than our local clusters can handle
		if atomic.LoadInt32(&redistributed) < redistributeQuota {
			searchCPU = 1 << 62
			searchMem = 1 << 62
		}

		// NextPendingJob atomically pops the best fit and locks it safely as "packing-in-progress"
		job, found := p.state.NextPendingJob(searchCPU, searchMem)
		if !found {
			break // Queue is empty, or no remaining jobs fit inside our search constraints
		}

		// Synchronous Pre-Calculation (Bin-Packing)
		var targetCluster string
		if numClusters > 0 {
			for j := 0; j < numClusters; j++ {
				targetIdx := (i + j) % numClusters
				cluster := clusters[targetIdx]

				if cluster.MaxCpu-cluster.UsedCpu >= job.CPURequirement && cluster.MaxMem-cluster.UsedMem >= job.MemoryRequirement {
					// Optimistically update the central state machine
					p.state.SoftUpdateCluster(cluster.ID, job.CPURequirement, job.MemoryRequirement)

					// Also update the local struct pointer in case background polling (UpdateCluster)
					// replaces the map pointer mid-loop, preventing 50-job batch bloat.
					cluster.UsedCpu += job.CPURequirement
					cluster.UsedMem += job.MemoryRequirement

					targetCluster = cluster.ID

					// Dynamically lower ceilings since we successfully reserved space
					maxCPU, maxMem = 0, 0
					for _, c := range clusters {
						if avail := c.MaxCpu - c.UsedCpu; avail > maxCPU {
							maxCPU = avail
						}
						if avail := c.MaxMem - c.UsedMem; avail > maxMem {
							maxMem = avail
						}
					}
					break
				}
			}
		}

		// Asynchronous Execution (Network/Raft I/O)
		wg.Add(1)
		sem <- struct{}{} // Block if 20 workers are already running

		go func(j *core.Job, target string) {
			defer wg.Done()
			defer func() { <-sem }() // Free the semaphore slot

			p.executeJobAssignment(ctx, j, target, &redistributed, redistributeQuota)
		}(job, targetCluster)
	}

	// Wait for this batch to complete before finishing the loop tick
	wg.Wait()
}

// executeJobAssignment abstracts the execution flow for a single job to keep the main loop clean.
func (p *Packer) executeJobAssignment(ctx context.Context, job *core.Job, targetCluster string, redistributed *int32, quota int32) {
	if targetCluster != "" {
		p.attemptJobAssignment(ctx, targetCluster, job)
		return
	}

	if atomic.LoadInt32(redistributed) < quota {
		atomic.AddInt32(redistributed, 1)
		if err := p.Redistribute(ctx, job); err == nil {
			return // Successfully transferred ownership and purged locally
		} else {
			glog.V(2).Infof("Redistribute job %s failed: %v", job.Id, err)
		}
	}

	// Only release the lock and return the job to the local pending queue if it wasn't redistributed
	p.state.UnassignJob(job.Id)
}

// calculateRedistributionQuota provides a smooth backpressure release valve for network load shedding.
func (p *Packer) calculateRedistributionQuota(jobCount int) int {
	quota := int(float64(jobCount) * 0.25)
	if quota < 5 {
		return 5 // Floor limit to guarantee small queues don't get trapped
	}
	return quota
}

// attemptJobAssignment secures the job assignment via Raft and dispatches it.
func (p *Packer) attemptJobAssignment(ctx context.Context, clusterId string, job *core.Job) bool {
	// Overwrites the "packing-in-progress" lock with the true target
	if err := p.state.AssignJob(job.Id, clusterId); err != nil {
		glog.Errorf("Assign job %s failed: %v", job.Id, err)
		return false
	}
	if err := p.sc.RaftConn.CommitJob(ctx, p.state.Id(), job); err != nil {
		glog.Warningf("Commit job %s failed: %v", job.Id, err)
		if errors.Is(err, core.ErrCASConcurrentConflict) {
			// Re-queue instead of purging so transient CAS conflicts don't permanently drop jobs
			p.state.UnassignJob(job.Id)
			glog.V(2).Infof("Job %s re-queued due to concurrent CAS conflict with peer", job.Id)
			return false
		}

		p.state.UnassignJob(job.Id) // Rollback!

		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.sc.RaftConn.CommitJob(rollbackCtx, p.state.Id(), job); err != nil {
			glog.Warningf("Rollback job %s failed: %v", job.Id, err)
		}
		return false
	}
	if exists, err := p.sc.Cluster(clusterId).DispatchJob(ctx, job); err != nil || !exists {
		p.state.UnassignJob(job.Id) // Rollback!

		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.sc.RaftConn.CommitJob(rollbackCtx, p.state.Id(), job); err != nil {
			glog.Warningf("Rollback job %s failed: %v", job.Id, err)
		}
		glog.Warningf("Dispatch job %s failed. Rolled back job.", job.Id)
		return false
	}
	glog.V(2).Infof("Job %s assigned to local cluster %s", job.Id, clusterId)
	return true
}

func (p *Packer) Redistribute(ctx context.Context, job *core.Job) error {
	peers := p.state.TopFreePeers(5, 5*time.Second, job.CPURequirement, job.MemoryRequirement)
	if len(peers) == 0 {
		return fmt.Errorf("no peers available with sufficient resources") // Returning error prevents limbo bugs
	}

	for _, bestFitId := range peers {
		// Directly send the full job with Gossip=false. The receiving handler checks capacity
		// before accepting, avoiding the double-RPC overhead and race window.
		if err := p.sc.CoordinateHandler.OpConn(ctx, bestFitId, func(ct context.Context, co *coordinate.Conn) error {
			ok, _, err := co.Redistribute(ct, p.state.Id(), job, false)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("peer rejected redistribution")
			}
			p.state.PurgeJob(job.Id)
			return nil
		}); err != nil {
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to redistribute job %s to any peer", job.Id)
}

// SendHeartbeats streams continuous health checks to coordinate clusters.
func (p *Packer) SendHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
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
