package states

import (
	"NotAHiveMind/internal/arbiter"
	"NotAHiveMind/internal/models"
	"NotAHiveMind/internal/scheduler/core"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// State is the central, thread-safe state machine for the Scheduler.
type State struct {
	id      string
	address string

	jobs map[string]*core.Job

	unassignedJobs *arbiter.Queue[*core.Job]
	assignedJobs   map[string]struct{}
	completedJobs  map[string]struct{}
	readJobs       map[string]time.Time

	clusters map[string]*ClusterState
	peers    []*SchedulerState

	mu sync.RWMutex
}

// ClusterStats represents a transient snapshot of cluster health
type ClusterStats struct {
	ClusterID   string
	CPUUsage    int64
	MemUsage    int64
	TotalCPU    int64
	TotalMem    int64
	LastUpdated time.Time
}

// NewState initializes a new central state machine and configures the generalized queue.
func NewState(id string, address string) *State {
	return &State{
		id:      id,
		address: address,
		jobs:    make(map[string]*core.Job),
		unassignedJobs: arbiter.NewQueue[*core.Job](
			func(j *core.Job) string { return j.Id },
			func(j *core.Job) int64 { return j.Priority },
			func(j *core.Job) int64 { return j.CPURequirement },
			func(j *core.Job) int64 { return j.MemoryRequirement },
		),
		assignedJobs:  make(map[string]struct{}),
		completedJobs: make(map[string]struct{}),
		readJobs:      make(map[string]time.Time),
		clusters:      make(map[string]*ClusterState),
		peers:         make([]*SchedulerState, 0),
	}
}

func (s *State) Id() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

func (s *State) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.address
}

func (s *State) AppendJob(job *core.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job.Status = models.Pending
	s.jobs[job.Id] = job
	s.unassignedJobs.Push(job)
}

// NextPendingJob atomically pops the best fitting job from the queue and temporarily marks it
// as "packing-in-progress" to prevent race conditions where jobs vanish into limbo.
func (s *State) NextPendingJob(availCPU, availMem int64) (*core.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.unassignedJobs.PopBestFit(availCPU, availMem)
	if !ok {
		return nil, false
	}

	// Immediately lock it safely into the active pool
	s.assignedJobs[job.Id] = struct{}{}
	job.AssignedClusterId = "packing-in-progress"
	job.Status = models.Running

	return job, true
}

func (s *State) AssignJob(jobId string, clusterId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobId]
	if !ok {
		return fmt.Errorf("job with ID %s not found", jobId)
	}

	// Redundant but safe (in case it bypassed NextPendingJob)
	s.unassignedJobs.RemoveByID(jobId)
	s.assignedJobs[jobId] = struct{}{}

	job.AssignedClusterId = clusterId
	job.Status = models.Running
	return nil
}

func (s *State) CompleteJob(jobId string, result json.RawMessage, success bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobId]
	if !ok {
		return fmt.Errorf("job with ID %s not found", jobId)
	}

	delete(s.assignedJobs, jobId)
	s.unassignedJobs.RemoveByID(jobId)
	s.completedJobs[jobId] = struct{}{}

	if success {
		job.Status = models.Completed
	} else {
		job.Status = models.Failed
	}

	job.Response = result
	return nil
}

// UnassignJob cleanly rolls a job back to the pending queue and strips its assignment state.
func (s *State) UnassignJob(jobId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobId]
	if !ok {
		return
	}

	if _, isCompleted := s.completedJobs[jobId]; isCompleted {
		return
	}

	delete(s.assignedJobs, jobId)
	delete(s.completedJobs, jobId)

	job.Status = models.Pending
	job.Response = nil
	job.AssignedClusterId = ""

	// Push back to the queue now that the properties are reset
	s.unassignedJobs.Push(job)
}

// PurgeJob completely removes a job from all memory maps and the queue.
func (s *State) PurgeJob(jobId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[jobId]; !ok {
		return
	}

	delete(s.jobs, jobId)
	s.unassignedJobs.RemoveByID(jobId)
	delete(s.assignedJobs, jobId)
	delete(s.completedJobs, jobId)
	delete(s.readJobs, jobId)
}

func (s *State) JobsInCluster(clusterId string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Pre-allocating the max possible size prevents costly slice resizing
	jobsInCluster := make([]string, 0, len(s.assignedJobs))
	for id := range s.assignedJobs {
		if job, ok := s.jobs[id]; ok && job.AssignedClusterId == clusterId {
			jobsInCluster = append(jobsInCluster, id)
		}
	}
	return jobsInCluster
}

func (s *State) Job(jobId string) (*core.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if job, ok := s.jobs[jobId]; ok {
		return job, nil
	}
	return nil, fmt.Errorf("job with ID %s not found", jobId)
}

func (s *State) PendingJobs() []string {
	// Delegated to the thread-safe O(N) map reader inside the queue
	return s.unassignedJobs.IDs()
}

func (s *State) ActiveJobs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	activeJobs := make([]string, 0, len(s.assignedJobs))
	for id := range s.assignedJobs {
		activeJobs = append(activeJobs, id)
	}
	return activeJobs
}

func (s *State) CompletedJobs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	completedJobs := make([]string, 0, len(s.completedJobs))
	for id := range s.completedJobs {
		completedJobs = append(completedJobs, id)
	}
	return completedJobs
}

// QueueFreeCompletedJob marks a completed job as "read" so that it can be gracefully purged later.
func (s *State) QueueFreeCompletedJob(jobId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[jobId]; !ok {
		return fmt.Errorf("job with ID %s not found", jobId)
	}

	if _, ok := s.readJobs[jobId]; !ok {
		s.readJobs[jobId] = time.Now()
	}
	return nil
}

func (s *State) ClearCompletedJobs(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removeList := make([]string, 0)

			// Use RLock to prevent pausing the scheduler while we iterate the map
			s.mu.RLock()
			for jobId := range s.completedJobs {
				// If we hit timeout, or we're starting to store way too many jobs, force-clear them
				if value, ok := s.readJobs[jobId]; ok && (time.Since(value) > 10*time.Second || len(s.completedJobs) > 100) {
					removeList = append(removeList, jobId)
				}
			}
			s.mu.RUnlock()

			// Purge handles its own Write Locks individually, preventing a giant system stall
			for _, jobId := range removeList {
				s.PurgeJob(jobId)
			}
		}
	}
}
