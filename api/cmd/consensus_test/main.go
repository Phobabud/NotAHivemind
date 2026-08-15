package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/models"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the gRPC connection and handles automatic leader routing.
type Client struct {
	conn       *grpc.ClientConn
	grpcClient pb.ConsensusCoordinationServiceClient
	address    string
}

// NewClient establishes an insecure gRPC connection to a target cluster cluster.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	return &Client{
		conn:       conn,
		grpcClient: pb.NewConsensusCoordinationServiceClient(conn),
		address:    addr,
	}, nil
}

// Close releases the underlying gRPC transport channels safely.
func (c *Client) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// SendJob dispatches a job to the cluster and handles dynamic leader redirection if necessary.
func (c *Client) SendJob(ctx context.Context, jobID string, img string, priority int, taskPayload string) error {
	rawTask, err := json.Marshal(map[string]string{"command": taskPayload})
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Pack the payload using the consensus model consumed by the state machine.
	jobPayload := models.JobPayload{
		ID:                jobID,
		State:             models.Pending,
		Image:             img,
		Priority:          priority,
		CPURequirement:    250,
		MemoryRequirement: 100,
		Payload:           rawTask,
	}

	serializedPayload, err := json.Marshal(jobPayload)
	if err != nil {
		return fmt.Errorf("failed to serialize job payload: %w", err)
	}

	req := &pb.RawAppendRequest{
		OriginNodeId: "consensus-test-client",
		RequestId:   fmt.Sprintf("req-%s-%d", jobID, time.Now().UnixNano()),
		SchedulerId: "test-client-scheduler-example",
		Payload:     serializedPayload,
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[Client] Sending job '%s' to cluster at %s (Attempt %d/%d)...", jobID, c.address, attempt, maxRetries)

		rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.grpcClient.ClientAppend(rpcCtx, req)
		cancel()

		if err != nil {
			log.Printf("[Client] gRPC network error from %s: %v", c.address, err)
			return err
		}

		switch resp.Status {
		case 1: // SUCCESS: Consensus committed and state machine applied
			log.Printf("[Client] SUCCESS: Job '%s' successfully committed to Raft group! RequestId: %s", jobID, resp.RequestId)
			return nil

		case 2: // REJECTED: Targeted cluster is not the active cluster leader
			if resp.LeaderAddress == "" {
				log.Printf("[Client] REJECTED: Node at %s is a Follower, but no leader is currently known. Backing off...", c.address)
				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf("[Client] REDIRECT: Node at %s is a Follower. Redirecting to current Leader at %s...", c.address, resp.LeaderAddress)

			// Dynamically pivot connection to the authoritative leader address
			newClient, err := NewClient(resp.LeaderAddress)
			if err != nil {
				log.Printf("[Client] Error establishing redirect link to leader at %s: %v", resp.LeaderAddress, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Swap connections
			c.Close()
			c.conn = newClient.conn
			c.grpcClient = newClient.grpcClient
			c.address = newClient.address

			// Instantly retry appending to the new Leader
			continue

		default:
			log.Printf("[Client] ERROR: Received unexpected status %d from cluster %s", resp.Status, c.address)
			return fmt.Errorf("unsupported append response status: %d", resp.Status)
		}
	}

	return fmt.Errorf("failed to commit job '%s' after %d redirect/retry attempts", jobID, maxRetries)
}

func main() {
	targetNode := flag.String("cluster", "localhost:50051", "Address of any cluster cluster to seed communication")
	flag.Parse()

	log.Printf("[Client] Initializing test client. Connecting to seed cluster: %s", *targetNode)
	client, err := NewClient(*targetNode)
	if err != nil {
		log.Fatalf("[Client] Fatal: Failed to initialize seed connection: %v", err)
	}
	defer client.Close()

	// List of test jobs simulating task scheduler-example actions
	jobsToSend := []struct {
		ID       string
		Image    string
		Priority int
		Command  string
	}{
		{ID: "job-001", Image: "ubuntu:latest", Priority: 3, Command: "echo 'Initializing Kubernetes Engine'"},
		{ID: "job-002", Image: "golang:1.22-alpine", Priority: 5, Command: "go test -v ./..."},
		{ID: "job-003", Image: "redis:alpine", Priority: 1, Command: "redis-server --protected-mode no"},
		{ID: "job-004", Image: "nginx:latest", Priority: 2, Command: "nginx -g 'daemon off;'"},
	}

	ctx := context.Background()

	log.Printf("[Client] Ready. Beginning batch dispatch of %d jobs...", len(jobsToSend))
	start := time.Now()

	successfulDispatches := 0
	for _, job := range jobsToSend {
		err := client.SendJob(ctx, job.ID, job.Image, job.Priority, job.Command)
		if err != nil {
			log.Printf("[Client] Failed to dispatch job '%s': %v", job.ID, err)
			continue
		}
		successfulDispatches++
		// Small spacing delay to simulate realistic ingestion
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[Client] Ingestion complete. Dispatched %d/%d jobs successfully in %v.",
		successfulDispatches, len(jobsToSend), time.Since(start))
}
