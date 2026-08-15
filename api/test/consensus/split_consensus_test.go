package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/consensus/filesystem"
	"NotAHiveMind/internal/consensus/service"
	"NotAHiveMind/internal/consensus/state"
	"context"
	"testing"
	"time"
)

type SplitTestNode struct {
	ID      string
	Port    string
	DataDir string
	Node    *service.Node
	Handler *filesystem.Handler
	Cancel  context.CancelFunc
}

func TestRaft5NodeSplitAndHeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"split-1", "split-2", "split-3", "split-4", "split-5"}
	ports := []string{":20051", ":20052", ":20053", ":20054", ":20055"}

	var testNodes []*SplitTestNode

	t.Logf("Booting the initial %d-Node Cluster...", len(nodeIDs))
	for i, id := range nodeIDs {
		dataDir := t.TempDir()
		handler, _ := filesystem.LoadStorage(dataDir, 10000)

		var peers []core.PeerState
		for j, peerID := range nodeIDs {
			if i != j {
				peers = append(peers, state.New(peerID, "127.0.0.1"+ports[j]))
			}
		}

		nodeCtx, nodeCancel := context.WithCancel(ctx)
		node := service.NewNode(nodeCtx, id, "127.0.0.1"+ports[i], handler, peers)

		t.Cleanup(func() {
			nodeCancel()
			node.Close()
			if err := handler.Close(); err != nil {
				t.Logf("Failed to close storage for %s: %v", id, err)
			}
		})

		testNodes = append(testNodes, &SplitTestNode{
			ID:      id,
			Port:    ports[i],
			DataDir: dataDir,
			Node:    node,
			Handler: handler,
			Cancel:  nodeCancel,
		})
	}

	// Wait for mesh and start elections
	t.Logf("Waiting for the %d-node TCP mesh to establish...", len(nodeIDs))
	time.Sleep(2 * time.Second)
	for _, n := range testNodes {
		n.Node.RecordContact()
		go runTestElectionLoop(ctx, n.Node)
	}

	// Find initial leader
	var leader *SplitTestNode
	var leaderIndex int

	initialPayload := []byte(`{"event": "initial-cluster-state"}`)
	initialReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-split-1",
		Payload:      initialPayload,
	}

	t.Log("Phase 2: Finding the Leader and establishing baseline consensus...")
	findLeaderCtx, findLeaderCancel := context.WithTimeout(ctx, 10*time.Second)
	defer findLeaderCancel()

	for leader == nil {
		select {
		case <-findLeaderCtx.Done():
			t.Fatalf("Failed to establish initial leader in the 5-node cluster")
		default:
			for i, n := range testNodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, initialReq)
				reqCancel()

				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					leader = n
					leaderIndex = i
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	t.Logf("SUCCESS: %s is the original Leader!", leader.ID)
	time.Sleep(200 * time.Millisecond) // Give replication time to flush to disks
	initialCommitIndex := leader.Handler.Index()

	// STONITH to 3 nodes to simulate catastrophic failure (leader and 2 random followers)
	t.Log("SIMULATING FAILURE. Killing 3 out of 5 nodes...")

	var deadIndices []int
	deadIndices = append(deadIndices, leaderIndex)
	for i := 0; i < 5; i++ {
		if len(deadIndices) == 3 {
			break
		}
		if i != leaderIndex {
			deadIndices = append(deadIndices, i)
		}
	}

	var deadNodes []*SplitTestNode
	var survivingNodes []*SplitTestNode

	for i, n := range testNodes {
		isDead := false
		for _, di := range deadIndices {
			if i == di {
				isDead = true
				break
			}
		}

		if isDead {
			t.Logf(" - Terminating %s...", n.ID)
			n.Cancel()
			n.Node.Close()
			if err := n.Handler.Close(); err != nil {
				t.Logf("Failed to close storage for %s: %v", n.ID, err)
			}
			deadNodes = append(deadNodes, n)
		} else {
			survivingNodes = append(survivingNodes, n)
		}
	}

	// Wait for gRPC timeouts to sever and election timers to trip on the 2 survivors
	t.Log("Waiting for the 2 remaining nodes to realize the cluster is dead...")
	time.Sleep(2 * time.Second)

	// Cluster Deadlock
	t.Log("Ensuring cluster is deadlocked due to the majority being offline...")

	splitPayload := []byte(`{"event": "split-brain-data"}`)
	splitReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-split-2",
		Payload:      splitPayload,
	}

	// Submit to the survivors. Should fail as quorum isn't reached
	for _, n := range survivingNodes {
		reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := n.Node.ClientAppend(reqCtx, splitReq)
		reqCancel()

		if err == nil && resp != nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
			t.Fatalf("CRITICAL SECURITY FAILURE: A minority of 2 nodes committed data without a quorum!")
		}
	}
	t.Log("SUCCESS: The cluster correctly gridlocked and protected itself from Split-Brain corruption.")

	// Revive dead nodes
	t.Log("Phase 5: HEALING THE PARTITION. Bringing the 3 dead nodes back online...")

	var resurrectedNodes []*SplitTestNode
	for _, dead := range deadNodes {
		// Re-initialize with their exact same directory and port
		resurrectedHandler, _ := filesystem.LoadStorage(dead.DataDir, 10000)

		var resurrectedPeers []core.PeerState
		for _, other := range testNodes {
			if other.ID != dead.ID {
				resurrectedPeers = append(resurrectedPeers, state.New(other.ID, "127.0.0.1"+other.Port))
			}
		}

		resurrectedCtx, resurrectedCancel := context.WithCancel(ctx)
		resurrectedNode := service.NewNode(resurrectedCtx, dead.ID, "127.0.0.1"+dead.Port, resurrectedHandler, resurrectedPeers)

		t.Cleanup(func() {
			resurrectedCancel()
			resurrectedNode.Close()
			if err := resurrectedHandler.Close(); err != nil {
				t.Logf("Failed to close storage for resurrected node %s: %v", dead.ID, err)
			}
		})

		wrap := &SplitTestNode{
			ID:      dead.ID,
			Port:    dead.Port,
			DataDir: dead.DataDir,
			Node:    resurrectedNode,
			Handler: resurrectedHandler,
			Cancel:  resurrectedCancel,
		}

		resurrectedNode.RecordContact()
		go runTestElectionLoop(ctx, resurrectedNode)
		resurrectedNodes = append(resurrectedNodes, wrap)
	}

	// Rebuild our active test slice with all 5 healthy nodes
	var finalNodes []*SplitTestNode
	finalNodes = append(finalNodes, survivingNodes...)
	finalNodes = append(finalNodes, resurrectedNodes...)

	// Wait for gRPC exponential backoff to recover and mesh to heal
	t.Log("Waiting 5 seconds for gRPC exponential backoff to recover and full mesh to heal...")
	time.Sleep(5 * time.Second)

	// Verify that the cluster has healed ana a leader exists
	t.Log("Phase 6: Submitting final data to verify the 5-node cluster elected a Leader and healed...")

	healPayload := []byte(`{"event": "healed-data"}`)
	healReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-split-3",
		Payload:      healPayload,
	}

	var newLeader *SplitTestNode
	newLeaderCtx, newLeaderCancel := context.WithTimeout(ctx, 10*time.Second)
	defer newLeaderCancel()

	for newLeader == nil {
		select {
		case <-newLeaderCtx.Done():
			t.Fatalf("The 5 nodes failed to recover and elect a Leader after being restarted!")
		default:
			for _, n := range finalNodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, healReq)
				reqCancel()

				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					newLeader = n
					break
				}
			}
		}
	}

	t.Logf("SUCCESS: The cluster healed and elected %s as the NEW Leader!", newLeader.ID)

	// Give the heartbeats a moment to replicate the final payload to all 5 disks
	time.Sleep(50 * time.Millisecond)

	finalLeaderIndex := newLeader.Handler.Index()
	if finalLeaderIndex <= initialCommitIndex {
		t.Fatalf("New leader did not advance the log index!")
	}

	for _, n := range finalNodes {
		if n.Handler.Index() != finalLeaderIndex {
			t.Errorf("FAILED: Node %s is stuck at index %d, expected %d", n.ID, n.Handler.Index(), finalLeaderIndex)
		}
	}

	t.Logf("SUCCESS: All 5 nodes flawlessly synchronized to Index %d!", finalLeaderIndex)
}
