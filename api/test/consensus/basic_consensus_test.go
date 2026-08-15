package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/consensus/filesystem"
	"NotAHiveMind/internal/consensus/service"
	"NotAHiveMind/internal/consensus/state"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNode keeps track of IDs since the actual struct is private
type TestNode struct {
	ID   string
	Node *service.Node
}

// runTestElectionLoop mimics the background loop from main.go
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

func TestRaftClusterIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	ports := []string{":20001", ":20002", ":20003"}

	var testNodes []*TestNode

	// Init and register temp dirs
	for i, id := range nodeIDs {
		dataDir := t.TempDir()

		handler, err := filesystem.LoadStorage(dataDir, 10000)
		if err != nil {
			t.Fatalf("Failed to load storage for %s: %v", id, err)
		}

		// Register the handler closure
		t.Cleanup(func() {
			if err := handler.Close(); err != nil {
				t.Logf("Failed to close storage for %s: %v", id, err)
			}
		})

		var peers []core.PeerState
		for j, peerID := range nodeIDs {
			if i != j {
				peers = append(peers, state.New(peerID, "127.0.0.1"+ports[j]))
			}
		}

		node := service.NewNode(ctx, id, "127.0.0.1"+ports[i], handler, peers)

		// Register the gRPC server shutdown immediately!
		t.Cleanup(func() {
			node.Close()
		})

		testNodes = append(testNodes, &TestNode{ID: id, Node: node})
	}

	// Wait to see if TCP connection are made
	t.Log("Waiting for cluster mesh to establish TCP connections...")
	for _, n := range testNodes {
		meshCtx, meshCancel := context.WithTimeout(ctx, 3*time.Second)
		for {
			if n.Node.NumberConnected() >= 2 { // Majority for a 3-node cluster
				meshCancel()
				break
			}
			select {
			case <-meshCtx.Done():
				t.Fatalf("Timeout waiting for node %s to connect to peers", n.ID)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	t.Log("Cluster mesh fully established!")

	// Boot elections
	for _, n := range testNodes {
		n.Node.RecordContact() // Reset their internal timeout clocks
		go runTestElectionLoop(ctx, n.Node)
	}

	// Attempt to broadcast a payload
	var leader *TestNode
	requireLeaderCtx, requireLeaderCancel := context.WithTimeout(ctx, 10*time.Second)
	defer requireLeaderCancel()

	testPayload := []byte(`{"command": "test-replication"}`)
	appendReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-12345",
		SchedulerId:  "sched-1",
		Payload:      testPayload,
	}

	lLoopBreak := true
	for lLoopBreak {
		select {
		case <-requireLeaderCtx.Done():
			t.Fatalf("Cluster failed to elect a leader and commit a payload within 10 seconds")
		default:
			// Ping every node attempting to append the data
			for _, n := range testNodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, appendReq)
				reqCancel()

				// If we hit the Leader, and it successfully achieved quorum, it returns Success!
				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					leader = n
					lLoopBreak = false
					break
				}
			}

			if lLoopBreak {
				time.Sleep(250 * time.Millisecond)
			}
		}
	}

	t.Logf("SUCCESS: %s was elected as the Leader and achieved quorum on the payload!", leader.ID)

	// Make sure everything has replicated
	time.Sleep(100 * time.Millisecond)

	for _, n := range testNodes {
		if n.ID == leader.ID {
			continue // We already know the leader succeeded
		}

		if n.Node.FileHandler.Index() < 1 {
			t.Errorf("Follower %s failed to replicate the log to disk. Current Index: %d", n.ID, n.Node.FileHandler.Index())
		} else {
			t.Logf("SUCCESS: Follower %s successfully replicated the log to index %d!", n.ID, n.Node.FileHandler.Index())
		}
	}
}

func TestRaftLogCompaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"comp-1", "comp-2", "comp-3"}
	ports := []string{":20011", ":20012", ":20013"}

	var testNodes []*TestNode
	var dataDirs []string

	for i, id := range nodeIDs {
		dataDir := t.TempDir()
		dataDirs = append(dataDirs, dataDir)

		handler, err := filesystem.LoadStorage(dataDir, 5)
		if err != nil {
			t.Fatalf("Failed to load storage for %s: %v", id, err)
		}

		t.Cleanup(func() {
			if err := handler.Close(); err != nil {
				t.Logf("Failed to close storage for %s: %v", id, err)
			}
		})

		go func() {
			if err := handler.StoreSnapshot(); err != nil {
				t.Logf("StoreSnapshot routine exited for %s: %v", id, err)
			}
		}()

		var peers []core.PeerState
		for j, peerID := range nodeIDs {
			if i != j {
				peers = append(peers, state.New(peerID, "127.0.0.1"+ports[j]))
			}
		}

		node := service.NewNode(ctx, id, "127.0.0.1"+ports[i], handler, peers)
		t.Cleanup(func() { node.Close() })

		testNodes = append(testNodes, &TestNode{ID: id, Node: node})
	}

	t.Log("Waiting for cluster mesh to establish TCP connections...")
	for _, n := range testNodes {
		meshCtx, meshCancel := context.WithTimeout(ctx, 3*time.Second)
		for {
			if n.Node.NumberConnected() >= 2 {
				meshCancel()
				break
			}
			select {
			case <-meshCtx.Done():
				t.Fatalf("Timeout waiting for node %s to connect to peers", n.ID)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	// Start Elections
	for _, n := range testNodes {
		n.Node.RecordContact()
		go runTestElectionLoop(ctx, n.Node)
	}

	t.Log("Flooding the cluster with 250 payloads to trigger the 5KB log rotation limit...")

	// ~250 payloads at ~100 bytes each = ~14.2 KB. This will force a snapshot!
	for i := 1; i <= 250; i++ {
		// We make valid JSON so createSnapshot can parse it
		payload := []byte(fmt.Sprintf(`{"id": "job-%d", "state": 1}`, i))

		appendReq := &pb.RawAppendRequest{
			OriginNodeId: "test-client",
			RequestId:    fmt.Sprintf("req-comp-%d", i),
			SchedulerId:  "sched-1",
			Payload:      payload,
		}

		// Keep pinging until ANY node accepts the data
		success := false
		for !success {
			for _, n := range testNodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, appendReq)
				reqCancel()

				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					success = true
					break
				}
			}

			if !success {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	t.Log("All data accepted! Waiting for background workers to compress and rotate logs...")

	// Verify the background process actually triggered
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 5*time.Second)
	defer timeoutCancel()

	snapshotVerified := false
	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("Timed out waiting for nodes to compact log. DiscIndex never advanced.")
		default:
			// Check if ALL nodes successfully compacted
			allCompacted := true
			for _, n := range testNodes {
				// If DiscIndex > 0, the snapshot worker successfully rotated the file
				if n.Node.FileHandler.DiscIndex() == 0 {
					allCompacted = false
					break
				}
			}

			if allCompacted {
				snapshotVerified = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}

		if snapshotVerified {
			break
		}
	}

	t.Log("SUCCESS: All nodes advanced their DiscIndex. Verifying physical filesystem...")

	// Ensure the compressed active.log actually exists on disk for every node
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
	// Fresh ports to avoid TCP TIME_WAIT collisions with the other tests
	ports := []string{":20031", ":20032", ":20033"}

	var testNodes []*TestNode
	var dataDirs []string

	// Setup data dirs
	for i := 0; i < 3; i++ {
		dataDirs = append(dataDirs, t.TempDir())
	}

	// Init only nodes 1 and 2
	for i := 0; i < 2; i++ {
		id := nodeIDs[i]

		// 5 KB Limit to easily trigger compaction
		handler, err := filesystem.LoadStorage(dataDirs[i], 5)
		if err != nil {
			t.Fatalf("Failed to load storage for %s: %v", id, err)
		}
		t.Cleanup(func() {
			if err := handler.Close(); err != nil {
				t.Logf("Failed to close storage for %s: %v", id, err)
			}
		})

		go func() {
			if err := handler.StoreSnapshot(); err != nil {
				t.Logf("Snapshot routine exited: %v", err)
			}
		}()

		var peers []core.PeerState
		for j, peerID := range nodeIDs {
			if i != j {
				peers = append(peers, state.New(peerID, "127.0.0.1"+ports[j]))
			}
		}

		node := service.NewNode(ctx, id, "127.0.0.1"+ports[i], handler, peers)
		t.Cleanup(func() { node.Close() })

		testNodes = append(testNodes, &TestNode{ID: id, Node: node})
	}

	// Wait for mesh between 1 & 2
	t.Log("Waiting for Nodes 1 and 2 to form a partial cluster...")
	for _, n := range testNodes {
		meshCtx, meshCancel := context.WithTimeout(ctx, 3*time.Second)
		for {
			if n.Node.NumberConnected() >= 1 {
				meshCancel()
				break
			}
			select {
			case <-meshCtx.Done():
				t.Fatalf("Timeout waiting for node %s to connect", n.ID)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	// Start Elections for 1 and 2
	for _, n := range testNodes {
		n.Node.RecordContact()
		go runTestElectionLoop(ctx, n.Node)
	}

	// Flood with 250 payloads
	t.Log("Flooding the partial cluster with 250 payloads to force a snapshot...")
	for i := 1; i <= 250; i++ {
		payload := []byte(fmt.Sprintf(`{"id": "job-%d", "state": 1}`, i))
		appendReq := &pb.RawAppendRequest{
			OriginNodeId: "test-client",
			RequestId:    fmt.Sprintf("req-sync-%d", i),
			SchedulerId:  "sched-1",
			Payload:      payload,
		}

		success := false
		for !success {
			for _, n := range testNodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, appendReq)
				reqCancel()

				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					success = true
					break
				}
			}
			if !success {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	t.Log("Waiting for Nodes 1 and 2 to compress logs and advance their snapshot boundaries...")
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 10*time.Second)
	defer timeoutCancel()
	snapshotVerified := false

	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("Nodes 1 and 2 failed to compact")
		default:
			allCompacted := true
			for _, n := range testNodes {
				if n.Node.FileHandler.DiscIndex() == 0 {
					allCompacted = false
					break
				}
			}
			if allCompacted {
				snapshotVerified = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if snapshotVerified {
			break
		}
	}
	t.Log("Nodes 1 and 2 compacted successfully! The early log entries are officially gone.")

	// Init late joiner (node 3)
	t.Log("Bringing Node 3 online. It is entirely blank and hundreds of indices behind...")

	handler3, err := filesystem.LoadStorage(dataDirs[2], 5)
	if err != nil {
		t.Fatalf("Failed to load storage for node 3: %v", err)
	}
	t.Cleanup(func() {
		if err := handler3.Close(); err != nil {
			t.Logf("Failed to close storage for node 3: %v", err)
		}
	})

	go func() {
		if err := handler3.StoreSnapshot(); err != nil {
			t.Logf("Node 3 Snapshot exited: %v", err)
		}
	}()

	var peers3 []core.PeerState
	for j, peerID := range nodeIDs {
		if 2 != j { // Node 3 is index 2
			peers3 = append(peers3, state.New(peerID, "127.0.0.1"+ports[j]))
		}
	}

	node3 := service.NewNode(ctx, nodeIDs[2], "127.0.0.1"+ports[2], handler3, peers3)
	t.Cleanup(func() { node3.Close() })
	node3Wrap := &TestNode{ID: nodeIDs[2], Node: node3}
	testNodes = append(testNodes, node3Wrap)

	node3.RecordContact()
	go runTestElectionLoop(ctx, node3)

	// Wait for all the nodes to discover each other
	t.Log("Waiting for full cluster mesh to heal so the Leader discovers Node 3...")
	discoveryCtx, discoveryCancel := context.WithTimeout(ctx, 10*time.Second)
	defer discoveryCancel()

	meshHealed := false
	for !meshHealed {
		select {
		case <-discoveryCtx.Done():
			t.Fatalf("Timeout waiting for cluster mesh to fully heal")
		default:
			connectedCount := 0
			for _, n := range testNodes {
				if n.Node.NumberConnected() >= 2 {
					connectedCount++
				}
			}
			if connectedCount == 3 {
				meshHealed = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	t.Log("Node 3 online and discovered! Submitting one final payload to forcibly trigger catch up")

	// Submit an additional payload to trigger catch-up
	triggerPayload := []byte(`{"id": "job-sync-trigger", "state": 1}`)
	triggerReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-sync-trigger",
		SchedulerId:  "sched-1",
		Payload:      triggerPayload,
	}

	success := false
	for !success {
		for _, n := range testNodes[:2] { // Ping the existing cluster to ensure we hit the leader
			reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			resp, err := n.Node.ClientAppend(reqCtx, triggerReq)
			reqCancel()
			if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
				success = true
				break
			}
		}
		if !success {
			time.Sleep(50 * time.Millisecond)
		}
	}

	t.Log("Waiting for the Leader to stream the binary snapshot chunk over gRPC...")

	// Verify Node 3 receives snapshot
	timeoutCtx2, timeoutCancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer timeoutCancel2()

	node3CaughtUp := false
	for {
		select {
		case <-timeoutCtx2.Done():
			t.Fatalf("Node 3 failed to install snapshot. DiscIndex is still %d", node3.FileHandler.DiscIndex())
		default:
			// If DiscIndex > 0, the chunk was successfully received, written to snapshot.tmp,
			// atomic swapped to active.log, and mounted into memory!
			if node3.FileHandler.DiscIndex() > 0 {
				node3CaughtUp = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if node3CaughtUp {
			break
		}
	}

	t.Logf("SUCCESS! Node 3 received the snapshot stream and jumped to DiscIndex %d!", node3.FileHandler.DiscIndex())

	// Verify physical filesystem on Node 3
	activeLogPath := filepath.Join(dataDirs[2], "active.log")
	stat, err := os.Stat(activeLogPath)
	if err != nil || stat.Size() == 0 {
		t.Errorf("Node 3 failed to create physical active.log file, or it was completely empty.")
	} else {
		t.Logf("SUCCESS: Node 3 reconstructed a compressed active.log of %d bytes from the stream!", stat.Size())
	}
}
