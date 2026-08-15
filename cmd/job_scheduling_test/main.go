package main

import (
	pb "NotAHiveMind/api/gen/scheduling/v1"
	"bytes"
	"context"
	"flag"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	// Allow the user to specify configuration
	targetsStr := flag.String("targets", "localhost:50050, localhost:50056", "Comma-separated list of scheduler addresses to connect to")
	numJobs := flag.Int("jobs", 10000, "The number of test jobs to submit")
	numWorkers := flag.Int("workers", 50, "The number of concurrent goroutines to spam the scheduler with")
	pollRate := flag.Int("poll-rate", 1, "Number of times per second to poll the ListJobs endpoint")
	flag.Parse()

	targetAddrs := strings.Split(*targetsStr, ",")
	log.Printf("Dialing %d schedulers to submit %d jobs using %d workers...", len(targetAddrs), *numJobs, *numWorkers)

	// Set up connections to all provided servers
	var conns []*grpc.ClientConn
	var clients []pb.SchedulingCoordinationServiceClient

	for _, addr := range targetAddrs {
		addr = strings.TrimSpace(addr)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Did not connect to %s: %v", addr, err)
		}
		conns = append(conns, conn)
		clients = append(clients, pb.NewSchedulingCoordinationServiceClient(conn))
	}

	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	// Prepare optional fields using pointers
	imageAlias := "go-echo-test"
	priority := int64(1)
	payload := []byte(`{"command": "echo 'Hello from the test client!'"}`)

	req := &pb.JobRequest{
		RequiredNanoCpu:     250000000,
		RequiredMemoryBytes: 104857600,
		ImageAlias:          &imageAlias,
		Priority:            &priority,
		Payload:             payload,
	}

	var activeJobIDs sync.Map
	var totalSubmitted int32

	log.Printf("Starting concurrent mass job submission. Sending ALL jobs to the first scheduler...")

	// Submission-------------------------------------------------------------------------------------------------------
	jobChan := make(chan int, *numJobs)
	for i := 1; i <= *numJobs; i++ {
		jobChan <- i
	}
	close(jobChan)

	var wg sync.WaitGroup
	var submitErrors int32

	// We'll use the first client for submission and polling
	client := clients[0]

	for w := 0; w < *numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobChan {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := client.RequestJob(ctx, req)
				cancel()

				if err != nil {
					errCount := atomic.AddInt32(&submitErrors, 1)
					if errCount <= 5 {
						log.Printf("[Phase 1 Error] Submission failed: %v", err)
					}
					continue
				}

				if resp.Accept && resp.JobId != "" {
					activeJobIDs.Store(resp.JobId, struct{}{})
					current := atomic.AddInt32(&totalSubmitted, 1)
					if current%100 == 0 {
						log.Printf("Submitted %d / %d jobs...", current, *numJobs)
					}
				}
			}
		}()
	}
	wg.Wait()

	actualSubmitted := atomic.LoadInt32(&totalSubmitted)
	log.Printf("Successfully submitted %d jobs. Failed Submissions: %d.", actualSubmitted, submitErrors)

	if actualSubmitted == 0 {
		log.Fatalf("No jobs were successfully submitted. Halting test.")
	}

	log.Printf("Beginning polling of ListJobs at %d req/sec...", *pollRate)

	// Ticker-based Polling of ListJobs---------------------------------------------------------------------------------
	tickerInterval := time.Second / time.Duration(*pollRate)
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	// Track jobs we've already cleaned up so we don't request them twice
	var cleanedUpJobs sync.Map
	var totalCleanedUp int32
	var totalFailed atomic.Int64

	for range ticker.C {
		if atomic.LoadInt32(&totalCleanedUp) >= actualSubmitted {
			break // All jobs reached a terminal state and were cleaned up!
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		listResp, err := client.ListJobs(ctx, &emptypb.Empty{})
		cancel()

		if err != nil {
			log.Printf("[Phase 2 Error] Failed to ListJobs: %v", err)
			continue
		}

		// Calculate how many of our submitted jobs are in each state based on the ListJobs response
		pendingCount := len(listResp.PendingJobId)
		runningCount := len(listResp.RunningJobId)
		completedCount := len(listResp.CompletedJobId)

		log.Printf("[LIVE DASHBOARD] Pending: %d | Running: %d | Completed (Waiting for Cleanup): %d | Total Jobs In System: %d | Cleaned Up: %d/%d | Failed: %d",
			pendingCount, runningCount, completedCount, listResp.TotalJobs, atomic.LoadInt32(&totalCleanedUp), actualSubmitted, totalFailed.Load())

		// Clean up completed jobs--------------------------------------------------------------------------------------
		if len(listResp.CompletedJobId) > 0 {
			var cleanupWg sync.WaitGroup

			for _, jobID := range listResp.CompletedJobId {
				// Only clean it up if it's one of OUR jobs, and we haven't already cleaned it up
				if _, isOurs := activeJobIDs.Load(jobID); isOurs {
					if _, alreadyCleaned := cleanedUpJobs.LoadOrStore(jobID, struct{}{}); !alreadyCleaned {
						cleanupWg.Add(1)
						go func(jid string) {
							defer cleanupWg.Done()
							statusCtx, statusCancel := context.WithTimeout(context.Background(), 2*time.Second)
							statusResp, err := client.RequestJobStatus(statusCtx, &pb.JobStatusRequest{JobId: jid})
							statusCancel()

							if err != nil {
								log.Printf("Failed to retrieve/cleanup completed job %s: %v", jid, err)
								// If it failed, remove it from cleanedUpJobs so we try again next tick
								cleanedUpJobs.Delete(jid)
								return
							}

							if statusResp.Status != "Completed" && statusResp.Status != "Failed" {
								log.Printf("Warning: Job %s was in Completed list but returned status %s", jid, statusResp.Status)
							}

							if !bytes.Equal(statusResp.Result, payload) {
								totalFailed.Add(1)
							}

							atomic.AddInt32(&totalCleanedUp, 1)
						}(jobID)
					}
				}
			}
			cleanupWg.Wait()
		}
	}

	log.Printf("All %d jobs have been completed and cleaned up. Test finished.", actualSubmitted)
	log.Printf("Total failed jobs: %d", totalFailed.Load())
}
