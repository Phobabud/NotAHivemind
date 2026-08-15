package scheduler

import (
	"NotAHiveMind/internal/cluster/core"
	"context"
	"encoding/json"
	"testing"
	"time"
)

type mockConfigLoader struct {
	core.ConfigLoader
	maxCpu int64
	maxMem int64
}

func (m *mockConfigLoader) GetMachineLimits() (int64, int64) { return m.maxCpu, m.maxMem }

type mockPackerContainer struct {
	core.ContainerAPI
	id       string
	reserved bool
}

func (m *mockPackerContainer) Id() string { return m.id }
func (m *mockPackerContainer) Reserve() bool {
	m.reserved = true
	return true
}
func (m *mockPackerContainer) Release()                                                  {}
func (m *mockPackerContainer) WaitUntilReady(ctx context.Context, d time.Duration) error { return nil }
func (m *mockPackerContainer) AssignJob(jobID string, payload json.RawMessage) error     { return nil }
func (m *mockPackerContainer) SetState(state core.ContainerState)                        {}
func (m *mockPackerContainer) WaitForJobResult(ctx context.Context) json.RawMessage      { return nil }
func (m *mockPackerContainer) FreeJob() error                                            { return nil }

type mockPackerPool struct {
	core.ContainerPool
	availableImages []string
	launchedIDs     []string
}

func (p *mockPackerPool) AvailableImageNames(ctx context.Context) []string { return p.availableImages }
func (p *mockPackerPool) AddFromImage(image *core.Image) (core.ContainerAPI, error) {
	return &mockPackerContainer{id: "new-container-123"}, nil
}
func (p *mockPackerPool) TrackedLaunch(ctx context.Context, id string) {
	p.launchedIDs = append(p.launchedIDs, id)
}
func (p *mockPackerPool) FindFreeContainer(ctx context.Context, name string) (core.ContainerAPI, error) {
	return nil, core.ErrAllOccupied
}

func TestPackNextJob_LaunchesNewContainer(t *testing.T) {
	config := &mockConfigLoader{maxCpu: 1000, maxMem: 1000}
	pool := &mockPackerPool{}

	// Load 1 job into the queue
	queue := CreateJobsQueue()
	job := core.NewJob("pack-job-1", &core.Image{Alias: "python", MemoryBytesLimit: 100, NanoCPULimit: 10}, 1, nil)
	_ = queue.AddJob(job)

	// Execute Packer
	err := PackNextJob(context.Background(), config, pool, queue)
	if err != nil {
		t.Fatalf("PackNextJob failed: %v", err)
	}

	// Ensure Queue and Pool transition properly
	if queue.LenPending() != 0 {
		t.Errorf("Expected job to leave Pending state")
	}
	if queue.LenActive() != 1 {
		t.Errorf("Expected job to transition to Active state")
	}
	if len(pool.launchedIDs) != 1 || pool.launchedIDs[0] != "new-container-123" {
		t.Errorf("Expected Packer to call AddFromImage and TrackedLaunch")
	}
}

func TestPackNextJob_RespectsLimits(t *testing.T) {
	// Config limits to 100 CPU / 100 Mem
	config := &mockConfigLoader{maxCpu: 100, maxMem: 100}
	pool := &mockPackerPool{}
	queue := CreateJobsQueue()

	// Add a massive job
	job := core.NewJob("massive-job", &core.Image{Alias: "python", MemoryBytesLimit: 5000, NanoCPULimit: 5000}, 1, nil)
	_ = queue.AddJob(job)

	err := PackNextJob(context.Background(), config, pool, queue)
	if err != nil {
		t.Fatalf("PackNextJob threw unexpected error: %v", err)
	}

	// Job should be completely ignored and rolled back because it doesn't fit!
	if queue.LenPending() != 1 || queue.LenActive() != 0 {
		t.Errorf("Expected massive job to be safely left in Pending state due to lack of resources")
	}
	if len(pool.launchedIDs) > 0 {
		t.Errorf("Expected no containers to be launched")
	}
}
