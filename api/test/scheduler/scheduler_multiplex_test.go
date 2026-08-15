package scheduler_integration_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/scheduler/service"
	"NotAHiveMind/internal/scheduler/states"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// MockRaftServer blindly responds to the GetData healthcheck so the Scheduler boots up
type MockRaftServer struct {
	pb.UnimplementedConsensusCoordinationServiceServer
}

func (m *MockRaftServer) GetData(ctx context.Context, req *pb.GetDataRequest) (*pb.GetDataResponse, error) {
	return &pb.GetDataResponse{Exists: true}, nil
}

// setupDummyServer creates a lightweight, empty gRPC server on a random open port
func setupDummyServer(t *testing.T, isRaft bool) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start dummy server listener: %v", err)
	}

	server := grpc.NewServer()

	// If this dummy server is meant to be the Raft node, attach our mock interface!
	if isRaft {
		pb.RegisterConsensusCoordinationServiceServer(server, &MockRaftServer{})
	}

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	return listener.Addr().String()
}

func TestSchedulerControlMultiplexer(t *testing.T) {
	// 1. SETUP: Create fake backend servers
	raftAddr := setupDummyServer(t, true) // Pass 'true' to register the Raft mock
	peerAddr1 := setupDummyServer(t, false)
	clusterAddr1 := setupDummyServer(t, false)
	clusterAddr2 := setupDummyServer(t, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. INITIALIZE: Boot the SchedulerControl
	dummyState := &states.State{}

	raftAddrs := []string{raftAddr}
	peers := map[string]string{"peer-1": peerAddr1}
	clusters := map[string]string{"cluster-1": clusterAddr1, "cluster-2": clusterAddr2}

	sc, err := service.NewSchedulerControl(ctx, dummyState, raftAddrs, peers, clusters)
	if err != nil {
		t.Fatalf("Failed to initialize SchedulerControl: %v", err)
	}

	// Ensure cleanup runs when the test ends
	t.Cleanup(func() {
		sc.Stop()
	})

	// 3. ASSERTIONS: Verify Initial State
	clusterIDs := sc.ClusterIDs()
	if len(clusterIDs) != 2 {
		t.Errorf("Expected 2 clusters to be registered, found %d", len(clusterIDs))
	}

	if conn := sc.Cluster("cluster-1"); conn == nil {
		t.Errorf("Expected to retrieve valid connection for cluster-1, got nil")
	}

	if conn := sc.Cluster("fake-cluster"); conn != nil {
		t.Errorf("Expected nil when requesting a non-existent cluster")
	}

	// 4. CONCURRENCY TORTURE TEST: Spam the Mutex
	t.Log("Beginning Mutex concurrency torture test...")

	var wg sync.WaitGroup
	numWorkers := 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			newClusterID := fmt.Sprintf("dynamic-cluster-%d", workerID)
			newClusterAddr := setupDummyServer(t, false) // Pass 'false' for regular dummy servers

			newMap := map[string]string{newClusterID: newClusterAddr}

			// This will lock s.mu and write
			sc.ConnectClusters(ctx, newMap)

			// This will lock s.mu and read
			_ = sc.Cluster(newClusterID)
		}(i)
	}

	// While the workers are writing, we continuously read to force a race condition
	for i := 0; i < 100; i++ {
		_ = sc.ClusterIDs()
		time.Sleep(1 * time.Millisecond)
	}

	wg.Wait()

	// 5. FINAL VERIFICATION
	finalIDs := sc.ClusterIDs()
	expectedTotal := 2 + numWorkers // The original 2 + the 50 dynamically added ones

	if len(finalIDs) != expectedTotal {
		t.Errorf("Mutex failed to safely register all dynamic clusters. Expected %d, got %d", expectedTotal, len(finalIDs))
	} else {
		t.Logf("SUCCESS: Mutex safely processed all concurrent cluster mappings. Total clusters: %d", len(finalIDs))
	}
}
