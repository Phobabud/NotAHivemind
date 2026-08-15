package scheduler_integration_test

import (
	pb "NotAHiveMind/api/gen/scheduling/v1"
	"NotAHiveMind/internal/scheduler/core"
	"NotAHiveMind/internal/scheduler/service/coordinate"
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// Attempt to mock peer to peer scheduler requests
type MockPeerServer struct {
	pb.UnimplementedSchedulingCoordinationServiceServer

	// Fields to capture what the client sent us
	ReceivedRedistribute *pb.RedistributeRequest
	ReceivedStatusReq    *pb.JobStatusRequest
}

func (m *MockPeerServer) Redistribute(ctx context.Context, req *pb.RedistributeRequest) (*pb.RedistributeResponse, error) {
	m.ReceivedRedistribute = req // Save it so the test can inspect it!

	return &pb.RedistributeResponse{
		JobId:               req.JobId,
		AssignedSchedulerId: "mock-peer-scheduler-1",
		Accept:              true,
	}, nil
}

func (m *MockPeerServer) RequestJobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	m.ReceivedStatusReq = req
	return &pb.JobStatusResponse{
		JobId:  req.JobId,
		Status: "RUNNING",
	}, nil
}

func TestCoordinateConn_NetworkBoundary(t *testing.T) {
	// Boot a mock server
	lis, err := net.Listen("tcp", "127.0.0.1:0") // Random free port
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	mockServer := &MockPeerServer{}
	pb.RegisterSchedulingCoordinationServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
	})

	// Connect cli to mock server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := coordinate.NewConn(ctx, "mock-peer", lis.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	// Wait for the background monitorConnection loop to see the connection is READY
	t.Log("Waiting for TCP connection to establish...")
	connected := false
	for i := 0; i < 50; i++ {
		if conn.IsConnected() {
			connected = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !connected {
		t.Fatalf("Client never successfully connected to the mock server")
	}

	// Test job redistribution
	t.Run("Redistribute Maps Job Correctly", func(t *testing.T) {
		dummyJob := &core.Job{
			Id:                "job-999",
			ImageAlias:        "ubuntu-worker",
			CPURequirement:    4000,
			MemoryRequirement: 8000,
			Priority:          10,
			Payload:           []byte("test-payload"),
		}

		accepted, assignID, err := conn.Redistribute(ctx, "test-origin-scheduler", dummyJob, true)
		if err != nil {
			t.Fatalf("Redistribute failed: %v", err)
		}

		if !accepted || assignID != "mock-peer-scheduler-1" {
			t.Errorf("Expected accepted=true, ID='mock-peer-scheduler-1', got %v, %s", accepted, assignID)
		}

		req := mockServer.ReceivedRedistribute
		if req == nil {
			t.Fatalf("Mock server never received the request!")
		}

		if req.JobId != dummyJob.Id {
			t.Errorf("Expected JobId %s, got %s", dummyJob.Id, req.JobId)
		}
		if *req.ImageAlias != dummyJob.ImageAlias {
			t.Errorf("Expected ImageAlias %s, got %s", dummyJob.ImageAlias, *req.ImageAlias)
		}
		if *req.Priority != int64(dummyJob.Priority) {
			t.Errorf("Expected Priority %d, got %d", dummyJob.Priority, *req.Priority)
		}
		if req.Gossip != true {
			t.Errorf("Expected Gossip to be true")
		}
	})

	// Test simple job status request
	t.Run("RequestJobStatus Maps Job Correctly", func(t *testing.T) {
		resp, err := conn.RequestJobStatus(ctx, "job-abc-123")
		if err != nil {
			t.Fatalf("RequestJobStatus failed: %v", err)
		}

		if resp.Status != "RUNNING" {
			t.Errorf("Expected status 'RUNNING', got %s", resp.Status)
		}

		if mockServer.ReceivedStatusReq == nil || mockServer.ReceivedStatusReq.JobId != "job-abc-123" {
			t.Errorf("Mock server received incorrect payload data")
		}
	})
}
