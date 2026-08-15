package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/consensus/filesystem"
	"NotAHiveMind/internal/consensus/service"
	"NotAHiveMind/internal/consensus/state"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type StressTestNode struct {
	ID      string
	Port    string
	DataDir string
	Node    *service.Node
	Handler *filesystem.Handler
	Cancel  context.CancelFunc
}

func TestRaftConcurrencyStress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"stress-1", "stress-2", "stress-3", "stress-4", "stress-5", "stress-6", "stress-7", "stress-8", "stress-9", "stress-10"}
	ports := []string{":20071", ":20072", ":20073", ":20074", ":20075", ":20076", ":20077", ":20078", ":20079", ":20080"}

	var testNodes []*StressTestNode

	t.Logf("Booting the %d-Node Cluster...", len(nodeIDs))
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

		testNodes = append(testNodes, &StressTestNode{
			ID:      id,
			Port:    ports[i],
			DataDir: dataDir,
			Node:    node,
			Handler: handler,
			Cancel:  nodeCancel,
		})
	}

	t.Log("Waiting for the TCP mesh to establish...")
	for _, n := range testNodes {
		meshCtx, meshCancel := context.WithTimeout(ctx, 3*time.Second)
		for {
			if n.Node.NumberConnected() >= 2 {
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

	for _, n := range testNodes {
		n.Node.RecordContact()
		go runTestElectionLoop(ctx, n.Node)
	}

	var leader *StressTestNode
	initialReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-stress-init",
		Payload:      []byte(`{"event": "init"}`),
	}

	t.Log("Finding the Leader...")
	findLeaderCtx, findLeaderCancel := context.WithTimeout(ctx, 10*time.Second)
	defer findLeaderCancel()

	response := false
	for leader == nil && !response {
		select {
		case <-findLeaderCtx.Done():
			t.Fatalf("Failed to establish initial leader")
		default:
			for _, n := range testNodes {
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				resp, err := n.Node.ClientAppend(reqCtx, initialReq)
				reqCancel()

				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					leader = n
					response = true
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	t.Logf("SUCCESS: %s is the Leader!", leader.ID)

	// Wait a moment for the initial payload to flush to disk
	time.Sleep(50 * time.Millisecond)
	initialCommitIndex := leader.Handler.Index()

	t.Log("CONCURRENCY STRESS TEST. Preparing to fire payloads")

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var failCount atomic.Int32

	numWorkers := 50
	requestsPerWorker := 200
	totalRequests := numWorkers * requestsPerWorker

	t.Logf("Firing %d total asynchronous payloads from %d goroutines...", totalRequests, numWorkers)

	startTime := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				reqID := fmt.Sprintf("req-stress-%d-%d", workerID, j)

				// Bypass CAS Conflict rejection
				payload := []byte(fmt.Sprintf(`{"stress_test_data": "%s"}`, reqID))

				req := &pb.RawAppendRequest{
					OriginNodeId: "stress-client",
					RequestId:    reqID,
					Payload:      payload,
				}

				// Generous timeout to allow the 100-buffer channel to process the pressure
				reqCtx, reqCancel := context.WithTimeout(ctx, 15*time.Second)
				resp, err := leader.Node.ClientAppend(reqCtx, req)
				reqCancel()

				if err == nil && resp.Status == pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(w)
	}

	// Wait for all 1,000 requests to be processed
	wg.Wait()
	duration := time.Since(startTime)

	t.Logf("VERIFICATION. %d requests processed in %v.", totalRequests, duration)

	if failCount.Load() > 0 {
		t.Fatalf("FAILED: %d requests were dropped or failed consensus! Mutex lock or Channel bottleneck detected.", failCount.Load())
	}

	if successCount.Load() != int32(totalRequests) {
		t.Fatalf("FAILED: Expected %d successful requests, got %d", totalRequests, successCount.Load())
	}

	t.Logf("SUCCESS: RequestRegistry perfectly mapped all %d asynchronous callbacks!", totalRequests)

	// Final Disk Verification
	expectedFinalIndex := initialCommitIndex + int64(totalRequests)

	for _, n := range testNodes {
		if n.Handler.Index() != expectedFinalIndex {
			t.Errorf("FAILED: Node %s is stuck at index %d, expected %d", n.ID, n.Handler.Index(), expectedFinalIndex)
		} else {
			t.Logf("SUCCESS: Node %s physically wrote all %d payloads to disk (Index: %d)!", n.ID, totalRequests, n.Handler.Index())
		}
	}
}
