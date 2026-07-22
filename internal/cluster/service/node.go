package service

import (
	pb "ClusterManager/api/gen/cluster/v1"
	"ClusterManager/internal/cluster/core"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// ClusterServer implements the pb.ClusterServiceServer interface.
// It holds the execution plane dependencies (queues, containers) and exposes them via gRPC.
type ClusterServer struct {
	pb.UnimplementedClusterServiceServer

	nodeID      string
	nodeAddress string
	totalCPU    int64
	totalMemory int64

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

	grpcServer := grpc.NewServer(grpc.KeepaliveParams(kasp))

	srv := &ClusterServer{
		nodeID:      nodeID,
		nodeAddress: address,
		totalCPU:    cpu,
		totalMemory: mem,
		jobQueue:    jq,
		containers:  cp,
		images:      images,
		statusCh:    statusCh,
		grpcServer:  grpcServer,
	}

	pb.RegisterClusterServiceServer(grpcServer, srv)
	return srv
}

// Start opens the TCP port and blocks while serving gRPC requests.
func (s *ClusterServer) Start() error {
	listener, err := net.Listen("tcp", s.nodeAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.nodeAddress, err)
	}

	log.Printf("Worker Node %s listening for Scheduler commands on %s", s.nodeID, s.nodeAddress)
	return s.grpcServer.Serve(listener)
}

// Stop gracefully drains active connections and shuts down the listener.
func (s *ClusterServer) Stop() {
	if s.grpcServer != nil {
		log.Printf("Gracefully shutting down gRPC server for node %s...", s.nodeID)
		s.grpcServer.GracefulStop()
	}
}
