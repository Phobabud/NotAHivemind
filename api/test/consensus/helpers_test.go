package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/consensus/filesystem"
	"NotAHiveMind/internal/consensus/service"
	"NotAHiveMind/internal/consensus/state"
	"context"
	"sync"
	"testing"
	"time"
)

type TestNode struct {
	ID           string
	Port         string
	DataDir      string
	Node         *service.Node
	Handler      *filesystem.Handler
	Cancel       context.CancelFunc
	shutdownOnce sync.Once
}

func (tn *TestNode) Shutdown(t *testing.T) {
	tn.shutdownOnce.Do(func() {
		tn.Cancel()
		tn.Node.Close()
		if tn.Handler != nil {
			if err := tn.Handler.Close(); err != nil {
				t.Logf("Failed to close storage for %s: %v", tn.ID, err)
			}
		}
	})
}

func runTestElectionLoop(ctx context.Context, n *service.Node) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.IsElectionTimeout() {
				n.StartElection(ctx)
			}
		}
	}
}

func createNode(ctx context.Context, t *testing.T, id, port, dataDir string, peers []core.PeerState, storageLimit int64, runSnapshotter bool) *TestNode {
	handler, err := filesystem.LoadStorage(dataDir, storageLimit)
	if err != nil {
		t.Fatalf("Failed to load storage for %s: %v", id, err)
	}

	if runSnapshotter {
		go func() {
			if err := handler.StoreSnapshot(); err != nil {
				t.Logf("Snapshot routine exited for %s: %v", id, err)
			}
		}()
	}

	nodeCtx, nodeCancel := context.WithCancel(ctx)
	node := service.NewNode(nodeCtx, id, port, handler, peers)

	tn := &TestNode{
		ID:      id,
		Port:    port,
		DataDir: dataDir,
		Node:    node,
		Handler: handler,
		Cancel:  nodeCancel,
	}

	t.Cleanup(func() {
		tn.Shutdown(t)
	})

	return tn
}

func setupNodes(ctx context.Context, t *testing.T, indicesToStart []int, nodeIDs, ports, dataDirs []string, storageLimit int64, runSnapshotter bool) []*TestNode {
	var nodes []*TestNode
	for _, i := range indicesToStart {
		var peers []core.PeerState
		for j, peerID := range nodeIDs {
			if i != j {
				peers = append(peers, state.New(peerID, "127.0.0.1"+ports[j]))
			}
		}
		nodes = append(nodes, createNode(ctx, t, nodeIDs[i], "127.0.0.1"+ports[i], dataDirs[i], peers, storageLimit, runSnapshotter))
	}
	return nodes
}

func waitForMesh(ctx context.Context, t *testing.T, nodes []*TestNode, minConnected int, timeout time.Duration) {
	t.Logf("Waiting for cluster mesh to establish TCP connections (min %d)...", minConnected)
	for _, n := range nodes {
		meshCtx, meshCancel := context.WithTimeout(ctx, timeout)
		connected := false
		for !connected {
			if n.Node.NumberConnected() >= minConnected {
				connected = true
				break
			}
			select {
			case <-meshCtx.Done():
				t.Fatalf("Timeout waiting for node %s to connect to peers", n.ID)
			case <-time.After(50 * time.Millisecond):
			}
		}
		meshCancel()
	}
	t.Log("Cluster mesh requirements met!")
}

func startElections(ctx context.Context, nodes []*TestNode) {
	for _, n := range nodes {
		n.Node.RecordContact()
		go runTestElectionLoop(ctx, n.Node)
	}
}

func commitPayload(ctx context.Context, t *testing.T, nodes []*TestNode, req *pb.RawAppendRequest, timeout time.Duration) *TestNode {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("Cluster failed to elect a leader and commit a payload within %v", timeout)
		default:
			for _, n := range nodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, req)
				reqCancel()

				if err == nil && resp != nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					return n
				}
			}
			time.Sleep(100 * time.Millisecond) // brief backoff before re-trying the cluster
		}
	}
}

func awaitLogCompaction(ctx context.Context, t *testing.T, nodes []*TestNode, timeout time.Duration) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("Timed out waiting for nodes to compact log. DiscIndex never advanced.")
		default:
			allCompacted := true
			for _, n := range nodes {
				if n.Handler.DiscIndex() == 0 {
					allCompacted = false
					break
				}
			}
			if allCompacted {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
