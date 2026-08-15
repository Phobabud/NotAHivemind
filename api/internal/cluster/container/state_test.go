package container

import (
	"NotAHiveMind/internal/cluster/core"
	"sync"
	"sync/atomic"
	"testing"
)

func TestContainer_ConcurrentReservations(t *testing.T) {
	c := &Container{
		id:    "test-concurrent-container",
		state: core.Free,
		mutex: &sync.RWMutex{},
	}

	var successCount int32
	var wg sync.WaitGroup
	numWorkers := 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.Reserve() {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Fatalf("Concurrency Failure: Expected exactly 1 successful reservation, got %d", successCount)
	}

	if c.State() != core.Reserved {
		t.Errorf("Expected container state to be RESERVED, got %v", c.State())
	}
}

func TestContainer_StateTransitions(t *testing.T) {
	c := &Container{
		id:              "test-lifecycle",
		dockerID:        "mock-docker-hash", // Required to bypass uninitialized checks
		payloadLocation: t.TempDir(),        // Safe sandbox for directory creation
		state:           core.Free,
		mutex:           &sync.RWMutex{},
	}

	if !c.Reserve() {
		t.Fatalf("Failed to transition from FREE to RESERVED")
	}

	dummyPayload := []byte(`{"task": "do_something"}`)
	err := c.AssignJob("job-123", dummyPayload)
	if err != nil {
		t.Fatalf("Failed to assign job: %v", err)
	}

	if c.State() != core.Running || c.JobID() != "job-123" {
		t.Errorf("Expected state RUNNING with job-123, got %v with %s", c.State(), c.JobID())
	}

	err = c.FreeJob()
	if err != nil {
		t.Fatalf("Failed to free job: %v", err)
	}

	if c.State() != core.Free || c.JobID() != "" {
		t.Errorf("Expected state FREE with empty JobID, got %v with %s", c.State(), c.JobID())
	}
}

func TestContainer_JobValidation(t *testing.T) {
	t.Run("Fails if uninitialized", func(t *testing.T) {
		c := &Container{
			id:       "test-dead",
			dockerID: "", // Simulating a container that failed to boot in Docker
			state:    core.Reserved,
			mutex:    &sync.RWMutex{},
		}

		err := c.AssignJob("job-123", []byte("{}"))
		if err == nil {
			t.Errorf("Expected error when assigning job to uninitialized container")
		}
	})

	t.Run("Fails if already running", func(t *testing.T) {
		c := &Container{
			id:       "test-occupied",
			dockerID: "mock-hash",
			state:    core.Running,
			mutex:    &sync.RWMutex{},
		}

		err := c.AssignJob("job-999", []byte("{}"))
		if err == nil {
			t.Errorf("Expected error when assigning job to an already RUNNING container")
		}
	})
}
