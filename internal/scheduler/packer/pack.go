package packer

import (
	"ClusterManager/internal/scheduler/service"
	"ClusterManager/internal/scheduler/states"
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
)

// Packer is the background worker responsible for pulling pending jobs from the state,
// finding a suitable local cluster, or shedding the load to a peer scheduler.
type Packer struct {
	sc    *service.SchedulerControl
	state *states.State

	activeEvictions sync.Map
}

// NewPacker initializes a new packing engine linked to the central control plane.
func NewPacker(control *service.SchedulerControl, state *states.State) *Packer {
	return &Packer{
		sc:    control,
		state: state,
	}
}

func (p *Packer) Start(ctx context.Context) {
	glog.V(1).Info("Starting Scheduler Packer loop...")

	go p.checkRaftConnection(ctx)
	go p.schedule(ctx)
	go p.SendHeartbeats(ctx)
}

func (p *Packer) schedule(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			glog.V(2).Info("Packer loop shutting down.")
			return
		case <-ticker.C:
			p.requestClusterState(ctx)
			p.reallocateDeadClusterJobs(ctx)
			p.checkSchedulers(ctx)

			p.processPendingJobs(ctx)
		}
	}
}
