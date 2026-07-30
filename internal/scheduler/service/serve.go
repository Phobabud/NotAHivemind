package service

import (
	pb "ClusterManager/api/gen/scheduling/v1"
	"ClusterManager/internal/scheduler/service/cluster"
	"ClusterManager/internal/scheduler/service/consensus"
	"ClusterManager/internal/scheduler/service/coordinate"
	"ClusterManager/internal/scheduler/states"
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type SchedulerControl struct {
	server *grpc.Server
	state  *states.State

	clusterConns      map[string]*cluster.Conn
	RaftConn          *consensus.RaftClient
	CoordinateHandler *coordinate.Handler
	mu                sync.Mutex
}

func NewSchedulerControl(ctx context.Context, state *states.State, raftAddrs []string, peerSchedulers map[string]string, clusters map[string]string) (*SchedulerControl, error) {
	raftClient, err := consensus.NewRaftClient(raftAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Raft client: %w", err)
	}

	peerConns := make([]*coordinate.Conn, 0, len(peerSchedulers))
	for id, addr := range peerSchedulers {
		conn, err := coordinate.NewConn(ctx, id, addr, state)
		if err != nil {
			glog.Warningf("Failed to initialize connection to peer scheduler at %s: %v", addr, err)
			continue
		}
		peerConns = append(peerConns, conn)
		glog.V(1).Infof("Successfully registered peer scheduler connection watchdog for %s", addr)
	}

	clusterConns := make(map[string]*cluster.Conn, len(clusters))
	for id, addr := range clusters {
		conn, err := cluster.NewConn(ctx, id, addr, state)
		if err != nil {
			glog.Warningf("Failed to initialize connection to cluster at %s: %v", addr, err)
			continue
		}
		clusterConns[id] = conn
		glog.V(1).Infof("Successfully registered cluster connection watchdog for %s", addr)
	}

	sc := &SchedulerControl{
		state:             state,
		clusterConns:      clusterConns,
		RaftConn:          raftClient,
		CoordinateHandler: coordinate.NewHandler(state, peerConns),
		mu:                sync.Mutex{},
	}

	// Initialize the underlying gRPC Server
	sc.server = grpc.NewServer()

	// Register our Coordination Handler to field client requests and peer gossip
	pb.RegisterSchedulingCoordinationServiceServer(sc.server, sc.CoordinateHandler)

	return sc, nil
}

// Start is a blocking call that binds the TCP port and starts serving requests.
func (s *SchedulerControl) Start() error {
	// Assumes s.state.Address() returns just the port string (e.g. "50050")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", s.state.Address()))
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", s.state.Address(), err)
	}

	glog.V(1).Infof("Scheduler gRPC Server multiplexing on port %s...", s.state.Address())
	return s.server.Serve(listener)
}

// Stop initiates a graceful shutdown of the gRPC server and all outbound connections.
func (s *SchedulerControl) Stop() {
	glog.V(1).Infof("Initiating graceful shutdown of SchedulerControl...")

	if s.server != nil {
		s.server.GracefulStop()
	}

	if s.RaftConn != nil {
		s.RaftConn.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, conn := range s.clusterConns {
		conn.Close()
		glog.V(2).Infof("Closed connection to cluster %s", id)
	}
}

// ClusterIDs returns a list of the cluster ids
func (s *SchedulerControl) ClusterIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.clusterConns))
	for id := range s.clusterConns {
		ids = append(ids, id)
	}
	return ids
}

// Cluster returns a pointer to a specific cluster's connection
func (s *SchedulerControl) Cluster(id string) *cluster.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn, ok := s.clusterConns[id]; ok {
		return conn
	}
	return nil
}

func (s *SchedulerControl) ConnectClusters(ctx context.Context, clusters map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clusterConns == nil {
		s.clusterConns = make(map[string]*cluster.Conn)
	}

	for id, addr := range clusters {
		if _, exists := s.clusterConns[id]; exists {
			continue
		}

		conn, err := cluster.NewConn(ctx, id, addr, s.state)
		if err != nil {
			glog.Errorf("Failed to initialize connection to cluster [%s] at [%s]: %v", id, addr, err)
			continue
		}

		s.clusterConns[id] = conn
		glog.V(1).Infof("Successfully registered cluster connection watchdog for %s at %s", id, addr)
	}
}
