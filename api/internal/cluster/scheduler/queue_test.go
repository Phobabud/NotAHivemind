package scheduler

import (
	"NotAHiveMind/internal/cluster/core"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestJobsQueue_BasicOperations(t *testing.T) {
	q := CreateJobsQueue()
	defer q.Close()

	dummyJob := core.NewJob("job-1", &core.Image{Alias: "test"}, 1, nil)

	if err := q.AddJob(dummyJob); err != nil {
		t.Fatalf("Failed to add job: %v", err)
	}

	if q.LenPending() != 1 {
		t.Errorf("Expected 1 pending job, got %d", q.LenPending())
	}

	if err := q.AddJob(dummyJob); !errors.Is(err, core.ErrAlreadyPending) {
		t.Errorf("Expected ErrAlreadyPending, got %v", err)
	}

	found, err := q.FindJob("job-1")
	if err != nil || found == nil {
		t.Errorf("Failed to find job after adding it")
	}

	if err := q.RemoveJob("job-1"); err != nil {
		t.Errorf("Failed to remove job: %v", err)
	}

	if q.LenPending() != 0 {
		t.Errorf("Expected queue to be empty after removal")
	}
}

func TestJobsQueue_NextBestJob_TieBreakers(t *testing.T) {
	q := CreateJobsQueue()
	defer q.Close()

	// Setup images
	imgSmall := &core.Image{Alias: "ubuntu", MemoryBytesLimit: 100, NanoCPULimit: 10}
	imgLarge := &core.Image{Alias: "ubuntu", MemoryBytesLimit: 500, NanoCPULimit: 10}
	imgPython := &core.Image{Alias: "python", MemoryBytesLimit: 100, NanoCPULimit: 10}
	imgTooLarge := &core.Image{Alias: "ubuntu", MemoryBytesLimit: 9000, NanoCPULimit: 10}

	_ = q.AddJob(core.NewJob("low-pri", imgSmall, 1, nil))
	_ = q.AddJob(core.NewJob("high-pri", imgSmall, 10, nil))
	_ = q.AddJob(core.NewJob("high-pri-large", imgLarge, 10, nil))
	_ = q.AddJob(core.NewJob("too-large", imgTooLarge, 99, nil)) // Should never be picked due to constraints
	_ = q.AddJob(core.NewJob("high-pri-matched", imgPython, 10, nil))

	var freeCpus int64 = 100
	var freeMem int64 = 1000
	freeContainers := []string{"python"} // Only python is actively free

	// Attempt to test tiebreaker (largest priority is way too large)
	best1 := q.NextBestJob(freeContainers, freeCpus, freeMem)
	if best1 == nil || best1.Id != "high-pri-matched" {
		t.Fatalf("Expected 'high-pri-matched', got %v", best1)
	}

	// Memory tiebreaker
	best2 := q.NextBestJob(freeContainers, freeCpus, freeMem)
	if best2 == nil || best2.Id != "high-pri-large" {
		t.Fatalf("Expected 'high-pri-large', got %v", best2)
	}

	best3 := q.NextBestJob(freeContainers, freeCpus, freeMem)
	if best3 == nil || best3.Id != "high-pri" {
		t.Fatalf("Expected 'high-pri', got %v", best3)
	}

	// Low priority left
	best4 := q.NextBestJob(freeContainers, freeCpus, freeMem)
	if best4 == nil || best4.Id != "low-pri" {
		t.Fatalf("Expected 'low-pri', got %v", best4)
	}

	// No jobs should be left
	best5 := q.NextBestJob(freeContainers, freeCpus, freeMem)
	if best5 != nil {
		t.Fatalf("Expected nil when no valid jobs remain, got %v", best5)
	}
}

func TestJobsQueue_StateMachine(t *testing.T) {
	q := CreateJobsQueue()
	defer q.Close()

	job := core.NewJob("job-state", &core.Image{Alias: "python", MemoryBytesLimit: 100}, 1, nil)
	_ = q.AddJob(job)

	// Claim Job (Moves Pending -> Active)
	best := q.NextBestJob([]string{}, 1000, 1000)
	if best == nil || q.LenPending() != 0 || q.LenActive() != 1 {
		t.Fatalf("Failed to transition job to Active list")
	}

	// Rollback (Moves Active -> Pending)
	if err := q.RollbackJob(best.Id); err != nil {
		t.Fatalf("Failed to rollback job: %v", err)
	}
	if q.LenPending() != 1 || q.LenActive() != 0 {
		t.Fatalf("Rollback failed to reset state correctly")
	}

	// Mark Completed (Moves Active -> Channel)
	_ = q.NextBestJob([]string{}, 1000, 1000) // Put it back in Active
	dummyPayload := json.RawMessage(`{"result": "success"}`)
	if err := q.MarkCompleted(best.Id, dummyPayload, true); err != nil {
		t.Fatalf("Failed to complete job: %v", err)
	}
	if q.LenActive() != 0 {
		t.Fatalf("Job leaked in Active map")
	}

	// Verify channel received the data without deadlocking
	select {
	case completed := <-q.Completed:
		if completed.Id != "job-state" || !completed.Succeeded {
			t.Errorf("Channel received invalid job data")
		}
	default:
		t.Errorf("Completed channel did not receive the job")
	}
}

func TestJobsQueue_ConcurrencyStress(t *testing.T) {
	q := CreateJobsQueue()
	defer q.Close()

	// Load 1,000 jobs with UNIQUE IDs
	for i := 0; i < 1000; i++ {
		jobID := fmt.Sprintf("job-concurrent-%d", i)
		_ = q.AddJob(core.NewJob(jobID, &core.Image{Alias: "test"}, 1, nil))
	}

	var wg sync.WaitGroup
	claimedJobs := make(chan *core.Job, 1000)

	// Simulate 50 concurrent Packers claiming jobs
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				job := q.NextBestJob([]string{}, 1000, 1000)
				if job != nil {
					claimedJobs <- job
				}
			}
		}()
	}

	wg.Wait()
	close(claimedJobs)

	count := 0
	for range claimedJobs {
		count++
	}

	if count != 1000 {
		t.Errorf("Concurrency failure: Expected 1000 claimed jobs, got %d", count)
	}
	if q.LenPending() != 0 || q.LenActive() != 1000 {
		t.Errorf("Queue internal state corrupted by concurrent map access")
	}
}
