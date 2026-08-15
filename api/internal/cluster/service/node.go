package service

import (
	pb "NotAHiveMind/api/gen/cluster/v1"
	"NotAHiveMind/internal/cluster/core"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// ClusterServer implements the pb.ClusterServiceServer interface.
// It holds the execution plane dependencies (queues, containers) and exposes them via gRPC.
type ClusterServer struct {
	pb.UnimplementedClusterServiceServer

	nodeID                   string
	nodeAddress              string
	totalCPU                 int64
	totalMemory              int64
	lastContactFromScheduler time.Time

	jobQueue   core.JobQueue
	containers core.ContainerPool
	images     []*core.Image
	statusCh   <-chan *core.Job

	grpcServer *grpc.Server
}

// NewClusterServer injects the core logic and configures the gRPC server.
func NewClusterServer(nodeID string, address string, cpu int64, mem int64, jq core.JobQueue, cp core.ContainerPool, images []*core.Image, statusCh <-chan *core.Job) *ClusterServer {
	kasp := keepalive.ServerParameters{
		Timeout: 1 * time.Second,
	}

	srv := &ClusterServer{
		nodeID:                   nodeID,
		nodeAddress:              address,
		totalCPU:                 cpu,
		totalMemory:              mem,
		lastContactFromScheduler: time.Now(),
		jobQueue:                 jq,
		containers:               cp,
		images:                   images,
		statusCh:                 statusCh,
		grpcServer:               nil,
	}

	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		srv.lastContactFromScheduler = time.Now()
		return handler(ctx, req)
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveParams(kasp), grpc.UnaryInterceptor(interceptor))
	srv.grpcServer = grpcServer

	pb.RegisterClusterServiceServer(grpcServer, srv)
	return srv
}

// Start opens the TCP port and blocks while serving gRPC requests.
func (s *ClusterServer) Start() error {
	listener, err := net.Listen("tcp", s.nodeAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.nodeAddress, err)
	}

	return s.grpcServer.Serve(listener)
}

// Stop gracefully drains active connections and shuts down the listener.
func (s *ClusterServer) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}

func (s *ClusterServer) MonitorConnection(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Since(s.lastContactFromScheduler) > 5*time.Second {
				glog.V(1).Infof("Scheduler connection lost for cluster, destroying %d containers", s.containers.Len())
				if s.containers.Len() == 0 {
					continue
				}
				if err := s.containers.DestroyAll(ctx); err != nil {
					glog.Errorf("Failed to destroy containers for cluster %s. Error: %v", s.nodeID, err)
				}
			}
		}
	}
}
