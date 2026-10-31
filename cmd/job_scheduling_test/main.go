package main

import (
	pb "NotAHiveMind/api/gen/scheduling/v1"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// schedStats acts as a thread-safe data container for our dashboard rows
type schedStats struct {
	addr      string
	pending   int
	running   int
	completed int // Currently waiting for cleanup
	cleanedUp int // Cumulative total of jobs cleaned up by this scheduler
	total     int
	err       error
}

func main() {
	// Allow the user to specify configuration
	targetsStr := flag.String("targets", "localhost:50050, localhost:50056", "Comma-separated list of scheduler addresses to connect to")
	numJobs := flag.Int("jobs", 1000, "The number of test jobs to submit")
	numWorkers := flag.Int("workers", 10, "The number of concurrent goroutines to spam the scheduler with")
	pollRate := flag.Int("poll-rate", 1, "Number of times per second to poll the ListJobs endpoint")
	flag.Parse()

	var targetAddrs []string
	for _, addr := range strings.Split(*targetsStr, ",") {
		targetAddrs = append(targetAddrs, strings.TrimSpace(addr))
	}

	log.Printf("Dialing %d schedulers to submit %d jobs using %d workers...", len(targetAddrs), *numJobs, *numWorkers)

	// Set up connections to all provided servers
	var conns []*grpc.ClientConn
	var clients []pb.SchedulingCoordinationServiceClient

	for _, addr := range targetAddrs {
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

	// We'll use the first client for submission
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

	// Dashboard Polling------------------------------------------------------------------------------------------------
	tickerInterval := time.Second / time.Duration(*pollRate)
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	// Track jobs we've already cleaned up so we don't request them twice
	var cleanedUpJobs sync.Map
	var totalCleanedUp int32
	var totalFailed atomic.Int64

	// Array to track historical cleanups per scheduler index
	cumulativeCleanedUp := make([]int32, len(clients))

	// Start the clock!
	startTime := time.Now()

	for range ticker.C {
		if atomic.LoadInt32(&totalCleanedUp) >= actualSubmitted {
			break // All jobs reached a terminal state and were cleaned up!
		}

		stats := make([]schedStats, len(clients))
		var listWg sync.WaitGroup

		// Fetch statuses from ALL schedulers concurrently
		for i, c := range clients {
			listWg.Add(1)
			go func(idx int, cClient pb.SchedulingCoordinationServiceClient, addr string) {
				defer listWg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				listResp, err := cClient.ListJobs(ctx, &emptypb.Empty{})
				cancel()

				if err != nil {
					stats[idx] = schedStats{addr: addr, err: err}
					return
				}

				stats[idx] = schedStats{
					addr:      addr,
					pending:   len(listResp.PendingJobId),
					running:   len(listResp.RunningJobId),
					completed: len(listResp.CompletedJobId),
					cleanedUp: int(atomic.LoadInt32(&cumulativeCleanedUp[idx])),
					total:     int(listResp.TotalJobs),
				}

				// Clean up completed jobs that reside on THIS specific scheduler
				if len(listResp.CompletedJobId) > 0 {
					var cleanupWg sync.WaitGroup

					for _, jobID := range listResp.CompletedJobId {
						if _, isOurs := activeJobIDs.Load(jobID); isOurs {
							if _, alreadyCleaned := cleanedUpJobs.LoadOrStore(jobID, struct{}{}); !alreadyCleaned {
								cleanupWg.Add(1)
								go func(jid string) {
									defer cleanupWg.Done()
									statusCtx, statusCancel := context.WithTimeout(context.Background(), 2*time.Second)

									// Target the exact scheduler holding the completed state
									statusResp, err := cClient.RequestJobStatus(statusCtx, &pb.JobStatusRequest{JobId: jid})
									statusCancel()

									if err != nil {
										log.Printf("Failed to retrieve/cleanup completed job %s from %s: %v", jid, addr, err)
										cleanedUpJobs.Delete(jid)
										return
									}

									if statusResp.Status != "Completed" && statusResp.Status != "Failed" {
										log.Printf("Warning: Job %s was in Completed list but returned status %s", jid, statusResp.Status)
									}

									if !bytes.Equal(statusResp.Result, payload) {
										totalFailed.Add(1)
									}

									// Increment global and scheduler-specific cleanup trackers
									atomic.AddInt32(&totalCleanedUp, 1)
									atomic.AddInt32(&cumulativeCleanedUp[idx], 1)
								}(jobID)
							}
						}
					}
					cleanupWg.Wait()
				}
			}(i, c, targetAddrs[i])
		}

		// Wait for all schedulers to return their telemetry and finish cleanup
		listWg.Wait()

		// Print the formatted table dashboard
		elapsed := time.Since(startTime).Round(time.Second)
		fmt.Printf("\n--- DASHBOARD (Elapsed: %v) ---\n", elapsed)
		fmt.Printf("Overall Progress: Completed: %d/%d | Failed: %d\n", atomic.LoadInt32(&totalCleanedUp), actualSubmitted, totalFailed.Load())
		fmt.Println("---------------------------------------------------------------------------------------------------------")

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
		fmt.Fprintln(w, "SCHEDULER\tPENDING\tRUNNING\tWAITING CLEANUP\tCLEANED UP (TOTAL)\tACTIVE IN SYSTEM\tSTATUS\t")

		for _, s := range stats {
			statusText := "OK"
			if s.err != nil {
				statusText = fmt.Sprintf("ERR: %v", s.err)
			}
			fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%s\t\n", s.addr, s.pending, s.running, s.completed, s.cleanedUp, s.total, statusText)
		}

		w.Flush()
		fmt.Println() // Add a newline of padding after the table
	}

	log.Printf("All %d jobs have been completed and cleaned up. Test finished in %v.", actualSubmitted, time.Since(startTime).Round(time.Second))
	log.Printf("Total failed jobs: %d", totalFailed.Load())
}
