package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"context"
	"testing"
	"time"
)

// To a node, a split is almost identical to killing off a massive chunk of nodes. This tests to see if a minority becomes rogue
func TestRaftNodeSplitAndHeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"split-1", "split-2", "split-3", "split-4", "split-5"}
	ports := []string{":20051", ":20052", ":20053", ":20054", ":20055"}
	dataDirs := []string{t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()}

	t.Logf("Booting the initial 5-Node Cluster...")
	testNodes := setupNodes(ctx, t, []int{0, 1, 2, 3, 4}, nodeIDs, ports, dataDirs, 10000, false)
	waitForMesh(ctx, t, testNodes, 2, 5*time.Second)
	startElections(ctx, testNodes)

	initialReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-split-1",
		Payload:      []byte(`{"event": "initial-cluster-state"}`),
	}

	t.Log("Phase 2: Finding the Leader and establishing baseline consensus...")
	leader := commitPayload(ctx, t, testNodes, initialReq, 10*time.Second)
	t.Logf("SUCCESS: %s is the original Leader!", leader.ID)

	time.Sleep(200 * time.Millisecond) // Let disks flush
	initialCommitIndex := leader.Handler.Index()

	t.Log("SIMULATING FAILURE. Killing 3 out of 5 nodes...")
	var deadIndices []int
	var survivingNodes []*TestNode

	// Find the leader's index to ensure we kill it
	for i, n := range testNodes {
		if n.ID == leader.ID {
			deadIndices = append(deadIndices, i)
			break
		}
	}
	// Append two more generic followers to kill
	for i := 0; i < 5; i++ {
		if len(deadIndices) == 3 {
			break
		}
		if i != deadIndices[0] {
			deadIndices = append(deadIndices, i)
		}
	}

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
			n.Shutdown(t)
		} else {
			survivingNodes = append(survivingNodes, n)
		}
	}

	t.Log("Waiting for the 2 remaining nodes to realize the cluster is dead...")
	time.Sleep(2 * time.Second)
	t.Log("Ensuring cluster is deadlocked due to the majority being offline...")

	splitReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-split-2",
		Payload:      []byte(`{"event": "split-brain-data"}`),
	}

	for _, n := range survivingNodes {
		reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := n.Node.ClientAppend(reqCtx, splitReq)
		reqCancel()
		if err == nil && resp != nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
			t.Fatalf("CRITICAL SECURITY FAILURE: A minority of 2 nodes committed data without a quorum!")
		}
	}
	t.Log("SUCCESS: The cluster correctly gridlocked and protected itself from Split-Brain corruption.")

	t.Log("Phase 5: HEALING THE PARTITION. Bringing the 3 dead nodes back online...")
	resurrectedNodes := setupNodes(ctx, t, deadIndices, nodeIDs, ports, dataDirs, 10000, false)
	startElections(ctx, resurrectedNodes)

	var finalNodes []*TestNode
	finalNodes = append(finalNodes, survivingNodes...)
	finalNodes = append(finalNodes, resurrectedNodes...)

	t.Log("Waiting for gRPC exponential backoff to recover and full mesh to heal...")
	waitForMesh(ctx, t, finalNodes, 4, 10*time.Second)
	time.Sleep(2 * time.Second) // Added padding for election safety

	t.Log("Phase 6: Submitting final data to verify the 5-node cluster elected a Leader and healed...")
	healReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-split-3",
		Payload:      []byte(`{"event": "healed-data"}`),
	}

	newLeader := commitPayload(ctx, t, finalNodes, healReq, 10*time.Second)
	t.Logf("SUCCESS: The cluster healed and elected %s as the NEW Leader!", newLeader.ID)

	time.Sleep(100 * time.Millisecond) // flush

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
