package cluster

import (
	pb "ClusterManager/api/gen/cluster/v1"
	"ClusterManager/internal/scheduler/core"
	"ClusterManager/internal/scheduler/states"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Conn wraps a raw gRPC connection to a Worker Cluster with automated
// state tracking, auto-reconnect logic, and data validation.
type Conn struct {
	mu sync.RWMutex

	nodeID  string
	address string
	state   *states.State // Injected state dependency

	client pb.ClusterServiceClient
	conn   *grpc.ClientConn

	connected bool
	cancel    context.CancelFunc
}

// NewConn dials the target cluster and spins up the background state watchdog.
func NewConn(ctx context.Context, nodeID, address string, state *states.State) (*Conn, error) {
	grpcConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial cluster %s at %s: %w", nodeID, address, err)
	}

	connCtx, cancel := context.WithCancel(ctx)

	c := &Conn{
		nodeID:  nodeID,
		address: address,
		state:   state, // Store the reference to the global state
		client:  pb.NewClusterServiceClient(grpcConn),
		conn:    grpcConn,
		cancel:  cancel,
	}

	go c.monitorConnection(connCtx)
	go c.monitorJobStatuses(connCtx)

	return c, nil
}

// Close gracefully tears down the TCP socket and stops background loops.
func (c *Conn) Close() {
	c.cancel()
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return
		}
	}
}

// monitorConnection tracks the underlying gRPC socket state. It forces reconnects
// if the connection idles or drops, and safely updates the 'connected' boolean.
func (c *Conn) monitorConnection(ctx context.Context) {
	if c.conn == nil {
		return
	}

	// Prompt the gRPC client to immediately start connection attempts
	c.conn.Connect()

	for {
		currentState := c.conn.GetState()
		if !c.conn.WaitForStateChange(ctx, currentState) {
			return
		}

		newState := c.conn.GetState()
		if newState == connectivity.Ready {
			if !c.connected {
				glog.V(2).Infof("Connection to cluster %s restored/active: %s", c.nodeID, newState)
				c.connected = true
			}
		} else if newState == connectivity.Idle || newState == connectivity.TransientFailure {
			c.conn.Connect()

			if c.connected {
				glog.Warningf("Connection to cluster %s dropped/inactive: %s", c.nodeID, newState)
				c.connected = false
			}
		}
	}
}

// Connection provides thread-safe access to the active connection state.
func (c *Conn) Connection() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// FetchClusterStatus pulls the telemetry heartbeat from the cluster
// and returns a clean domain model instead of the raw protobuf.
func (c *Conn) FetchClusterStatus(ctx context.Context) (*Status, error) {
	if !c.Connection() {
		return nil, fmt.Errorf("cannot fetch status: cluster %s is offline", c.nodeID)
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := c.client.ClusterStatus(rpcCtx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return ParseClusterStatus(resp), nil
}

// DispatchJob enforces strict data validation before sending scheduling intents over the wire.
// It accepts a core.Job domain model and handles the translation internally.
func (c *Conn) DispatchJob(ctx context.Context, job core.Job) (bool, error) {
	if !c.Connection() {
		return false, fmt.Errorf("cannot dispatch job %s: cluster %s is offline", job.Id, c.nodeID)
	}

	if job.Id == "" {
		return false, fmt.Errorf("malformed request: job ID cannot be empty")
	}
	if job.ImageAlias == "" {
		return false, fmt.Errorf("malformed request: image alias cannot be empty for job %s", job.Id)
	}
	if job.CPULimit <= 0 || job.MemoryLimit <= 0 {
		return false, fmt.Errorf("malformed request: CPU and Memory requirements must be > 0 for job %s", job.Id)
	}

	req := &pb.JobRequest{
		JobId:             job.Id,
		CpuRequirement:    job.CPULimit,
		MemoryRequirement: job.MemoryLimit,
		ImageAlias:        job.ImageAlias,
		Priority:          int64(job.Priority),
		Payload:           job.Payload,
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := c.client.AppendJob(rpcCtx, req)
	if err != nil {
		return false, fmt.Errorf("gRPC error appending job to %s: %w", c.nodeID, err)
	}

	if resp.Accept {
		// Update the global state immediately if the cluster accepts the job
		if err := c.state.AssignJob(job.Id, c.nodeID); err != nil {
			glog.Errorf("Failed to update state for assigned job %s: %v", job.Id, err)
		}
	}

	return resp.Accept, nil
}

// monitorJobStatuses opens a resilient, long-lived server stream to listen for terminal job events.
// It automatically translates the events and updates the injected state machine.
func (c *Conn) monitorJobStatuses(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !c.Connection() {
			time.Sleep(2 * time.Second)
			continue
		}

		glog.V(2).Infof("Opening JobStatus stream to cluster %s...", c.nodeID)

		stream, err := c.client.JobStatus(ctx, &emptypb.Empty{})
		if err != nil {
			glog.Errorf("Failed to open JobStatus stream to %s: %v. Retrying in 3s...", c.nodeID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					glog.V(2).Infof("JobStatus stream closed cleanly by cluster %s", c.nodeID)
					break
				}
				glog.Warningf("JobStatus stream to %s interrupted: %v", c.nodeID, err)
				break
			}

			event := ParseJobEvent(resp)
			if event == nil {
				continue
			}

			if event.Status == "COMPLETED" || event.Status == "FAILED" {
				err := c.state.CompleteJob(event.JobID, event.Payload, event.Status == "COMPLETED")
				if err != nil {
					glog.Errorf("Failed to process job completion for %s: %v", event.JobID, err)
				} else {
					glog.Infof("Job %s processed successfully (Status: %s)", event.JobID, event.Status)
				}
			}
		}

		time.Sleep(1 * time.Second)
	}
}
