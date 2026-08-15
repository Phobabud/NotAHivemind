package consensus_integraton_test

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRaftConcurrencyStress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs := []string{"stress-1", "stress-2", "stress-3", "stress-4", "stress-5", "stress-6", "stress-7", "stress-8", "stress-9", "stress-10"}
	ports := []string{":20071", ":20072", ":20073", ":20074", ":20075", ":20076", ":20077", ":20078", ":20079", ":20080"}

	var dataDirs []string
	var indices []int
	for i := 0; i < 10; i++ {
		dataDirs = append(dataDirs, t.TempDir())
		indices = append(indices, i)
	}

	t.Logf("Booting the 10-Node Cluster...")
	testNodes := setupNodes(ctx, t, indices, nodeIDs, ports, dataDirs, 10000, false)
	waitForMesh(ctx, t, testNodes, 2, 10*time.Second)
	startElections(ctx, testNodes)

	initialReq := &pb.RawAppendRequest{
		OriginNodeId: "test-client",
		RequestId:    "req-stress-init",
		Payload:      []byte(`{"event": "init"}`),
	}

	t.Log("Finding the Leader...")
	leader := commitPayload(ctx, t, testNodes, initialReq, 10*time.Second)
	t.Logf("SUCCESS: %s is the Leader!", leader.ID)

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
				req := &pb.RawAppendRequest{
					OriginNodeId: "stress-client",
					RequestId:    reqID,
					Payload:      []byte(fmt.Sprintf(`{"stress_test_data": "%s"}`, reqID)),
				}

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
	expectedFinalIndex := initialCommitIndex + int64(totalRequests)

	for _, n := range testNodes {
		if n.Handler.Index() != expectedFinalIndex {
			t.Errorf("FAILED: Node %s is stuck at index %d, expected %d", n.ID, n.Handler.Index(), expectedFinalIndex)
		} else {
			t.Logf("SUCCESS: Node %s physically wrote all %d payloads to disk (Index: %d)!", n.ID, totalRequests, n.Handler.Index())
		}
	}
}
