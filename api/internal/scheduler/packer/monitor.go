package packer

import (
	"NotAHiveMind/internal/models"
	"context"
	"time"

	"github.com/golang/glog"
)

func (p *Packer) checkRaftConnection(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	count := 0
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.sc.RaftConn.ConnectionStatus(ctx) {
				count = 0 // Reset on successful connection
				continue
			}

			count++
			glog.Warningf("Failed to reach Raft quorum. Strike %d of 3.", count)

			if count >= 3 {
				glog.Fatalf("Lost Raft! Killing scheduler.")
				return
			}
		}
	}
}

func (p *Packer) checkSchedulers(ctx context.Context) {
	staleSchedulers := p.state.StaleSchedulers(5 * time.Second)
	if len(staleSchedulers) == 0 {
		return
	}

	for _, schedulerID := range staleSchedulers {
		if _, ok := p.activeEvictions.Load(schedulerID); ok {
			continue
		}
		go p.attemptSchedulerTakeover(ctx, schedulerID)
	}
}

func (p *Packer) requestClusterState(ctx context.Context) {
	for _, cluster := range p.state.LocalClusters() {
		status, err := p.sc.Cluster(cluster.ID).FetchClusterStatus(ctx)
		if err != nil {
			return
		}
		p.state.UpdateCluster(cluster.ID, int64(status.TotalCPU-status.AvailableCPU), int64(status.TotalMemory-status.AvailableMemory), int64(status.TotalCPU), int64(status.TotalMemory))
	}
}

func (p *Packer) attemptSchedulerTakeover(ctx context.Context, schedulerID string) {
	p.activeEvictions.Store(schedulerID, struct{}{})
	defer p.activeEvictions.Delete(schedulerID)

	if err := p.sc.RaftConn.CommitSchedulerEvent(ctx, p.state.Id(), schedulerID, models.HeartbeatFailed.String()); err != nil {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	mostRecentEvent, err := p.sc.RaftConn.FetchSchedulerEvent(ctx, p.state.Id(), schedulerID)
	if err != nil {
		return
	}
	if mostRecentEvent.OriginSchedulerID != schedulerID || mostRecentEvent.Entry == models.Online.String() {
		return
	}
	if err := p.sc.RaftConn.CommitSchedulerEvent(ctx, p.state.Id(), schedulerID, models.RedistributedJobs.String()); err != nil {
		return
	}
}

func (p *Packer) reallocateDeadClusterJobs(ctx context.Context) {
	staleClusters := p.state.StaleClusters(5 * time.Second)
	for _, staleClusterID := range staleClusters {
		jobIDs := p.state.JobsInCluster(staleClusterID)
		for _, jobID := range jobIDs {
			p.state.UnassignJob(jobID)
		}
	}
}
