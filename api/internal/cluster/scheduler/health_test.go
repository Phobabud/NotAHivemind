package scheduler

import (
	"NotAHiveMind/internal/cluster/core"
	"context"
	"testing"
	"time"
)

type mockHealthContainer struct {
	core.ContainerAPI // Embedded interface ignores methods we don't define
	id                string
	state             core.ContainerState
	lastStateChange   time.Time
	totalJobs         int
	healthStatus      int
	healthErr         error
	image             *core.Image
	cleanupCalled     bool
}

// Mock the interface to allow for tests
func (m *mockHealthContainer) Id() string                 { return m.id }
func (m *mockHealthContainer) State() core.ContainerState { return m.state }
func (m *mockHealthContainer) LastStateChange() time.Time { return m.lastStateChange }
func (m *mockHealthContainer) TotalJobs() int             { return m.totalJobs }
func (m *mockHealthContainer) Image() *core.Image         { return m.image }
func (m *mockHealthContainer) JobID() string              { return "test-job-123" }
func (m *mockHealthContainer) Health(ctx context.Context) (int, error) {
	return m.healthStatus, m.healthErr
}
func (m *mockHealthContainer) Cleanup(ctx context.Context) error {
	m.cleanupCalled = true
	return nil
}

type mockHealthPool struct {
	core.ContainerPool // Embedded
	containers         map[string]*mockHealthContainer
	removedIDs         []string
}

func (p *mockHealthPool) ForEach(action func(container core.ContainerAPI) error) error {
	for _, c := range p.containers {
		if err := action(c); err != nil {
			return err
		}
	}
	return nil
}
func (p *mockHealthPool) Remove(id string) error {
	p.removedIDs = append(p.removedIDs, id)
	return nil
}

func TestHealthRuleUnresponsive(t *testing.T) {
	rule := HealthRuleUnresponsive()

	// Should Delete (Status 2 = Unhealthy)
	c1 := &mockHealthContainer{state: core.Running, healthStatus: 2}
	if rule(context.Background(), c1, nil) != HealthDelete {
		t.Errorf("Expected Unhealthy container to be marked for deletion")
	}

	// Should Skip (Status 0 = Healthy)
	c2 := &mockHealthContainer{state: core.Running, healthStatus: 0}
	if rule(context.Background(), c2, nil) != HealthContinue {
		t.Errorf("Expected Healthy container to continue")
	}

	// Should Skip (Reserved containers shouldn't be assassinated while booting)
	c3 := &mockHealthContainer{state: core.Reserved, healthStatus: 3}
	if rule(context.Background(), c3, nil) != HealthContinue {
		t.Errorf("Expected Reserved container to be protected from deletion")
	}
}

func TestHealthRuleTimedOut(t *testing.T) {
	rule := HealthRuleTimedOut()
	img := &core.Image{Timeout: 10} // 10 second timeout

	// Should Delete (idle for 20 seconds)
	c1 := &mockHealthContainer{
		state:           core.Free,
		totalJobs:       1,
		lastStateChange: time.Now().Add(-20 * time.Second),
		image:           img,
	}
	if rule(context.Background(), c1, nil) != HealthDelete {
		t.Errorf("Expected Stale/Timed-out container to be marked for deletion")
	}

	// Should Skip (Sitting FREE for only 5 seconds)
	c2 := &mockHealthContainer{
		state:           core.Free,
		totalJobs:       1,
		lastStateChange: time.Now().Add(-5 * time.Second),
		image:           img,
	}
	if rule(context.Background(), c2, nil) != HealthContinue {
		t.Errorf("Expected fresh container to continue")
	}
}

func TestRunHealthRule(t *testing.T) {
	// We use the real JobsQueue to test the MarkCompleted side-effect safely!
	queue := CreateJobsQueue()
	_ = queue.AddJob(core.NewJob("test-job-123", &core.Image{}, 1, nil))
	_ = queue.MarkRunning("test-job-123", "c-delete")

	cDelete := &mockHealthContainer{id: "c-delete", state: core.Running, healthStatus: 2}
	cSafe := &mockHealthContainer{id: "c-safe", state: core.Running, healthStatus: 0}

	pool := &mockHealthPool{
		containers: map[string]*mockHealthContainer{"c-delete": cDelete, "c-safe": cSafe},
	}

	// Run Engine
	err := RunHealthRule(context.Background(), pool, queue, HealthRuleUnresponsive())
	if err != nil {
		t.Fatalf("RunHealthRule failed: %v", err)
	}

	// Verify Cleanup Side Effects
	if !cDelete.cleanupCalled {
		t.Errorf("Expected Cleanup() to be called on deleted container")
	}
	if cSafe.cleanupCalled {
		t.Errorf("Expected Safe container to be untouched")
	}
	if len(pool.removedIDs) != 1 || pool.removedIDs[0] != "c-delete" {
		t.Errorf("Expected pool.Remove() to be called on c-delete to clear map")
	}

	// Verify Job Side Effects (Deleted container should have its job aborted)
	if queue.LenActive() != 0 {
		t.Errorf("Expected aborted job to be cleared from Active map")
	}
}
