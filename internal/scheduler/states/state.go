package states

import (
	"ClusterManager/internal/scheduler/core"
	"encoding/json"
	"fmt"
	"sync"
)

type State struct {
	id      string
	address string

	jobs map[string]core.Job

	unassignedJobs map[string]struct{}
	assignedJobs   map[string]struct{}
	completedJobs  map[string]struct{}

	mu sync.RWMutex
}

func NewState(id string, address string) *State {
	return &State{
		id:             id,
		address:        address,
		jobs:           make(map[string]core.Job),
		unassignedJobs: make(map[string]struct{}),
		assignedJobs:   make(map[string]struct{}),
		completedJobs:  make(map[string]struct{}),
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

func (s *State) AppendJob(job core.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job.Status = core.Pending
	s.jobs[job.Id] = job
	s.unassignedJobs[job.Id] = struct{}{}
}

func (s *State) AssignJob(jobId string, clusterId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[jobId]; !ok {
		return fmt.Errorf("job with ID %s not found", jobId)
	}

	delete(s.unassignedJobs, jobId)
	s.assignedJobs[jobId] = struct{}{}

	job := s.jobs[jobId]
	job.AssignedClusterId = clusterId
	job.Status = core.Running
	s.jobs[jobId] = job
	return nil
}

func (s *State) CompleteJob(jobId string, result json.RawMessage, success bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[jobId]; !ok {
		return fmt.Errorf("job with ID %s not found", jobId)
	}

	delete(s.assignedJobs, jobId)
	s.completedJobs[jobId] = struct{}{}

	job := s.jobs[jobId]
	if success {
		job.Status = core.Completed
	} else {
		job.Status = core.Failed
	}
	job.Response = result
	s.jobs[jobId] = job
	return nil
}

// UnassignJob rolls a job back to pending
func (s *State) UnassignJob(jobId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[jobId]; !ok {
		return
	}

	delete(s.assignedJobs, jobId)
	delete(s.completedJobs, jobId)
	s.unassignedJobs[jobId] = struct{}{}

	job := s.jobs[jobId]
	job.Status = core.Pending
	job.Response = nil
	job.AssignedClusterId = ""
	s.jobs[jobId] = job
}

// PurgeJob completely removes a job from memory
func (s *State) PurgeJob(jobId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[jobId]; !ok {
		return
	}

	delete(s.jobs, jobId)
	delete(s.unassignedJobs, jobId)
	delete(s.assignedJobs, jobId)
	delete(s.completedJobs, jobId)
}

func (s *State) JobsInCluster(clusterId string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var jobsInCluster []string
	for id := range s.assignedJobs {
		if job, ok := s.jobs[id]; ok && job.AssignedClusterId == clusterId {
			jobsInCluster = append(jobsInCluster, id)
		}
	}
	return jobsInCluster
}
