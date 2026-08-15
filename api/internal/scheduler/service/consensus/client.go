package consensus

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftClient manages a single, persistent connection to the active Raft Leader.
// It automatically handles leader-election redirects, connection swapping, and payload interpretation.
type RaftClient struct {
	mu sync.RWMutex

	address     string
	addressPool []string
	conn        *grpc.ClientConn
	healing     int32

	client pb.ConsensusCoordinationServiceClient
}

// NewRaftClient connects to a known Raft seed cluster to bootstrap the connection.
func NewRaftClient(addresses []string) (*RaftClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no raft seed addresses provided")
	}

	rc := &RaftClient{
		addressPool: addresses,
	}

	// Use our self-healing loop to find the first active cluster
	if err := rc.healConnection(); err != nil {
		return nil, fmt.Errorf("failed to bootstrap connection to raft cluster: %w", err)
	}

	return rc, nil
}

// healConnection iterates through the address pool to find a stable connection.
func (c *RaftClient) healConnection() error {
	if !atomic.CompareAndSwapInt32(&c.healing, 0, 1) {
		return nil
	}
	defer atomic.StoreInt32(&c.healing, 0)

	c.mu.RLock()
	pool := c.addressPool
	c.mu.RUnlock()

	for _, addr := range pool {
		if err := c.switchConnection(addr); err != nil {
			continue
		}

		// Test the newly established socket with a lightweight read operation
		// (it doesn't do anything, just sees if the conn exists)
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		req := &pb.GetDataRequest{OriginNodeId: "connection-check"}
		_, err := client.GetData(ctx, req)
		cancel()

		if err == nil {
			glog.V(1).Infof("Raft client healed. Attached to active cluster at [%s]", addr)
			return nil
		}
	}

	return fmt.Errorf("exhausted address pool; no raft nodes are reachable")
}

func (c *RaftClient) monitorConnection(ctx context.Context) {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		state := conn.GetState()

		if state == connectivity.Ready {
			if !conn.WaitForStateChange(ctx, state) {
				glog.V(2).Infof("Shutting down connection monitor for self-healing consensus")
				return
			}
			continue
		}

		if state == connectivity.Idle || state == connectivity.TransientFailure {
			conn.Connect()
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		changed := conn.WaitForStateChange(timeoutCtx, state)
		cancel()

		if !changed {
			if ctx.Err() != nil {
				return // Main context cancelled, shut down cleanly
			}

			glog.Warningf("Raft connection stuck in %s. Triggering background heal...", state)
			go c.healConnection()

			time.Sleep(1 * time.Second)
		}
	}
}

// switchConnection securely tears down the old TCP socket and establishes a new one.
func (c *RaftClient) switchConnection(newAddr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Cleanly sever the old connection to avoid leaking file descriptors
	if c.conn != nil {
		c.conn.Close()
	}

	// Dial the new leader
	conn, err := grpc.NewClient(newAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial raft cluster at %s: %w", newAddr, err)
	}

	c.address = newAddr
	c.conn = conn
	c.client = pb.NewConsensusCoordinationServiceClient(conn)

	glog.V(1).Infof("Raft client established connection to cluster at [%s]", newAddr)
	return nil
}

// Close gracefully shuts down the client during scheduler termination.
func (c *RaftClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *RaftClient) ConnectionStatus(ctx context.Context) bool {
	if atomic.LoadInt32(&c.healing) == 1 {
		return true
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return false
	}

	state := conn.GetState()
	// TransientFailure means the connection is physically broken
	if state == connectivity.TransientFailure {
		return false
	}

	return true
}
