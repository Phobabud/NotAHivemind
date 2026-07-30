package coordinate

import (
	pb "ClusterManager/api/gen/scheduling/v1"
	"ClusterManager/internal/scheduler/core"
	"ClusterManager/internal/scheduler/states"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Conn wraps a raw gRPC connection to a Peer Scheduler.
type Conn struct {
	mu sync.RWMutex

	nodeID  string
	address string
	state   *states.State

	client pb.SchedulingCoordinationServiceClient
	conn   *grpc.ClientConn

	connected bool
	cancel    context.CancelFunc
}

// NewConn dials the target peer scheduler and starts the state watchdog.
func NewConn(ctx context.Context, nodeID, address string, state *states.State) (*Conn, error) {
	grpcConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer scheduler %s at %s: %w", nodeID, address, err)
	}

	connCtx, cancel := context.WithCancel(ctx)

	c := &Conn{
		nodeID:  nodeID,
		address: address,
		state:   state,
		client:  pb.NewSchedulingCoordinationServiceClient(grpcConn),
		conn:    grpcConn,
		cancel:  cancel,
	}

	go c.monitorConnection(connCtx)

	return c, nil
}

// Close gracefully tears down the TCP socket to the peer.
func (c *Conn) Close() {
	c.cancel()
	if c.conn != nil {
		c.conn.Close()
	}
}

// monitorConnection tracks the gRPC socket state for peer-to-peer gossip.
func (c *Conn) monitorConnection(ctx context.Context) {
	for {
		state := c.conn.GetState()

		c.mu.Lock()
		if state == connectivity.Ready {
			if !c.connected {
				glog.V(2).Infof("Peer scheduler connection to %s (%s) is READY", c.nodeID, c.address)
				c.connected = true
			}
		} else {
			if c.connected {
				glog.Warningf("Peer scheduler connection to %s (%s) dropped: %s", c.nodeID, c.address, state)
				c.connected = false
			}

			if state == connectivity.Idle || state == connectivity.TransientFailure {
				c.conn.Connect()
			}
		}
		c.mu.Unlock()

		if !c.conn.WaitForStateChange(ctx, state) {
			glog.V(2).Infof("Shutting down connection monitor for peer scheduler %s", c.nodeID)
			return
		}
	}
}

// IsConnected provides thread-safe access to the active connection state.
func (c *Conn) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// Redistribute passes a job to a peer scheduler (e.g., if this scheduler lacks capacity).
func (c *Conn) Redistribute(ctx context.Context, originID string, job *core.Job, gossip bool) (bool, string, error) {
	if !c.IsConnected() {
		return false, "", fmt.Errorf("cannot redistribute job %s: peer %s is offline", job.Id, c.nodeID)
	}

	alias := job.ImageAlias
	priority := int64(job.Priority)

	req := &pb.RedistributeRequest{
		OriginSchedulerId: originID,
		JobId:             job.Id,
		Gossip:            gossip,
		CpuRequirement:    job.CPURequirement,
		MemoryRequirement: job.MemoryRequirement,
		ImageAlias:        &alias,
		Priority:          &priority,
		Payload:           job.Payload,
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := c.client.Redistribute(rpcCtx, req)
	if err != nil {
		return false, "", fmt.Errorf("failed to redistribute job to %s: %w", c.nodeID, err)
	}

	return resp.Accept, resp.AssignedSchedulerId, nil
}

// RequestJobStatus asks a peer scheduler for the status of a specific job.
func (c *Conn) RequestJobStatus(ctx context.Context, jobID string) (*pb.JobStatusResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("cannot fetch job status for %s: peer %s is offline", jobID, c.nodeID)
	}

	req := &pb.JobStatusRequest{
		JobId: jobID,
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	return c.client.RequestJobStatus(rpcCtx, req)
}

// SendHeartbeat proactively transmits this scheduler's known cluster load out to a peer.
func (c *Conn) SendHeartbeat(ctx context.Context) error {
	if !c.IsConnected() {
		return fmt.Errorf("cannot send heartbeat: peer %s is offline", c.nodeID)
	}

	var pbUsages []*pb.ClusterUsage
	for _, u := range c.state.LocalClusters() {
		pbUsages = append(pbUsages, &pb.ClusterUsage{
			ClusterId: u.ID,
			CpuUsage:  u.UsedCpu,
			MemUsage:  u.UsedMem,
			TotalCpu:  u.MaxCpu,
			TotalMem:  u.MaxMem,
		})
	}

	req := &pb.HeartbeatRequest{
		SchedulerId:  c.state.Id(),
		ClusterUsage: pbUsages,
	}

	// Heartbeats should be fast and fail-fast
	rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err := c.client.Heartbeat(rpcCtx, req)
	return err
}
