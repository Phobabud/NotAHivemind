package service

import (
	"ClusterManager/internal/scheduler/service/cluster"
	"ClusterManager/internal/scheduler/states"
	"context"
	"fmt"
	"go/types"
	"net"
	"sync"

	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type SchedulerControl struct {
	server *grpc.Server
	state  *states.State

	clusterConns   map[string]*cluster.Conn
	raftConn       types.Nil
	coordinateConn map[string]types.Nil
	mu             sync.Mutex
}

func NewSchedulerControl(state *states.State) *SchedulerControl {
	return &SchedulerControl{
		state:          state,
		clusterConns:   make(map[string]*cluster.Conn),
		coordinateConn: make(map[string]types.Nil),
		mu:             sync.Mutex{},
	}
}

// start is a blocking call
func (s *SchedulerControl) start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", s.state.Address()))
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", s.state.Address(), err)
	}

	// TODO: add handlers

	fmt.Printf("Scheduler gRPC Server multiplexing on port %s...\n", s.state.Address())
	return s.server.Serve(listener)
}

func (s *SchedulerControl) ConnectClusters(ctx context.Context, clusters map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure the map is initialized
	if s.clusterConns == nil {
		s.clusterConns = make(map[string]*cluster.Conn)
	}

	for id, addr := range clusters {
		if _, exists := s.clusterConns[id]; exists {
			continue
		}

		conn, err := cluster.NewConn(ctx, id, addr, s.state)
		if err != nil {
			glog.Infof("[SchedulerControl] Failed to initialize connection to cluster %s at %s: %v", id, addr, err)
			continue
		}

		s.clusterConns[id] = conn
		glog.Infof("[SchedulerControl] Successfully registered cluster connection watchdog for %s at %s", id, addr)
	}
}
