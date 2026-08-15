package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRaftClusterIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	ports := []string{":20001", ":20002", ":20003"}
	dataDirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}

	testNodes := setupNodes(ctx, t, []int{0, 1, 2}, nodeIDs, ports, dataDirs, 10000, false)
	waitForMesh(ctx, t, testNodes, 2, 5*time.Second)
	startElections(ctx, testNodes)

	// Attempt to broadcast a payload
	appendReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-12345",
		SchedulerId:  "sched-1",
		Payload:      []byte(`{"command": "test-replication"}`),
	}

	leader := commitPayload(ctx, t, testNodes, appendReq, 10*time.Second)
	t.Logf("SUCCESS: %s was elected as the Leader and achieved quorum on the payload!", leader.ID)

	time.Sleep(100 * time.Millisecond) // Flush replication to disk

	for _, n := range testNodes {
		if n.ID == leader.ID {
			continue
		}
		if n.Handler.Index() < 1 {
			t.Errorf("Follower %s failed to replicate the log to disk. Current Index: %d", n.ID, n.Handler.Index())
		} else {
			t.Logf("SUCCESS: Follower %s successfully replicated the log to index %d!", n.ID, n.Handler.Index())
		}
	}
}

func TestRaftLogCompaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"comp-1", "comp-2", "comp-3"}
	ports := []string{":20011", ":20012", ":20013"}
	dataDirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}

	testNodes := setupNodes(ctx, t, []int{0, 1, 2}, nodeIDs, ports, dataDirs, 5, true)
	waitForMesh(ctx, t, testNodes, 2, 5*time.Second)
	startElections(ctx, testNodes)

	t.Log("Flooding the cluster with 250 payloads to trigger the 5KB log rotation limit...")
	for i := 1; i <= 250; i++ {
		appendReq := &pb.RawAppendRequest{
			OriginNodeId: "test-client",
			RequestId:    fmt.Sprintf("req-comp-%d", i),
			SchedulerId:  "sched-1",
			Payload:      []byte(fmt.Sprintf(`{"id": "job-%d", "state": 1}`, i)),
		}
		commitPayload(ctx, t, testNodes, appendReq, 5*time.Second)
	}

	t.Log("All data accepted! Waiting for background workers to compress and rotate logs...")
	awaitLogCompaction(ctx, t, testNodes, 5*time.Second)
	t.Log("SUCCESS: All nodes advanced their DiscIndex. Verifying physical filesystem...")

	for i, n := range testNodes {
		activeLogPath := filepath.Join(dataDirs[i], "active.log")
		stat, err := os.Stat(activeLogPath)
		if err != nil || stat.Size() == 0 {
			t.Errorf("Node %s failed to create physical active.log file, or it was completely empty.", n.ID)
		} else {
			t.Logf("SUCCESS: Node %s produced a compressed active.log of %d bytes!", n.ID, stat.Size())
		}
	}
}

func TestRaftSnapshotInstallation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"sync-1", "sync-2", "sync-3"}
	ports := []string{":20031", ":20032", ":20033"}
	dataDirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}

	// Init only nodes 1 and 2
	activeNodes := setupNodes(ctx, t, []int{0, 1}, nodeIDs, ports, dataDirs, 5, true)
	waitForMesh(ctx, t, activeNodes, 1, 5*time.Second)
	startElections(ctx, activeNodes)

	t.Log("Flooding the partial cluster with 250 payloads to force a snapshot...")
	for i := 1; i <= 250; i++ {
		appendReq := &pb.RawAppendRequest{
			OriginNodeId: "test-client",
			RequestId:    fmt.Sprintf("req-sync-%d", i),
			SchedulerId:  "sched-1",
			Payload:      []byte(fmt.Sprintf(`{"id": "job-%d", "state": 1}`, i)),
		}
		commitPayload(ctx, t, activeNodes, appendReq, 5*time.Second)
	}

	t.Log("Waiting for Nodes 1 and 2 to compress logs and advance their snapshot boundaries...")
	awaitLogCompaction(ctx, t, activeNodes, 10*time.Second)
	t.Log("Nodes 1 and 2 compacted successfully! The early log entries are officially gone.")

	t.Log("Bringing Node 3 online. It is entirely blank and hundreds of indices behind...")
	node3Slice := setupNodes(ctx, t, []int{2}, nodeIDs, ports, dataDirs, 5, true)
	activeNodes = append(activeNodes, node3Slice[0])
	startElections(ctx, node3Slice) // Only start loops for node 3

	t.Log("Waiting for full cluster mesh to heal so the Leader discovers Node 3...")
	waitForMesh(ctx, t, activeNodes, 2, 10*time.Second)
	t.Log("Node 3 online and discovered! Submitting one final payload to forcibly trigger catch up")

	triggerReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-sync-trigger",
		SchedulerId:  "sched-1",
		Payload:      []byte(`{"id": "job-sync-trigger", "state": 1}`),
	}
	commitPayload(ctx, t, activeNodes[:2], triggerReq, 5*time.Second)

	t.Log("Waiting for the Leader to stream the binary snapshot chunk over gRPC...")
	awaitLogCompaction(ctx, t, node3Slice, 5*time.Second) // Wait for DiscIndex > 0
	t.Logf("SUCCESS! Node 3 received the snapshot stream and jumped to DiscIndex %d!", node3Slice[0].Handler.DiscIndex())

	activeLogPath := filepath.Join(dataDirs[2], "active.log")
	stat, err := os.Stat(activeLogPath)
	if err != nil || stat.Size() == 0 {
		t.Errorf("Node 3 failed to create physical active.log file, or it was completely empty.")
	} else {
		t.Logf("SUCCESS: Node 3 reconstructed a compressed active.log of %d bytes from the stream!", stat.Size())
	}
}
