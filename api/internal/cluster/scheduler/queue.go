package scheduler

import (
	"NotAHiveMind/internal/cluster/core"
	"encoding/json"
	"fmt"
	"sync"
)

// Jobs implements core.JobQueue using highly optimized O(1) maps.
type Jobs struct {
	pending map[string]*core.Job
	active  map[string]*core.Job

	Completed chan *core.Job
	mu        *sync.Mutex
}

func CreateJobsQueue() *Jobs {
	return &Jobs{
		pending:   make(map[string]*core.Job),
		active:    make(map[string]*core.Job),
		Completed: make(chan *core.Job, 1000),
		mu:        new(sync.Mutex),
	}
}

func (j *Jobs) AddJob(job *core.Job) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, exists := j.pending[job.Id]; exists {
		return core.ErrAlreadyPending
	}
	if _, exists := j.active[job.Id]; exists {
		return core.ErrAlreadyRunning
	}

	j.pending[job.Id] = job
	return nil
}

func (j *Jobs) RemoveJob(id string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, ok := j.active[id]; ok {
		return fmt.Errorf("job %s is already running and cannot be removed", id)
	}
	if _, ok := j.pending[id]; ok {
		delete(j.pending, id)
		return nil
	}

	return fmt.Errorf("job %s does not exist", id)
}

func (j *Jobs) FindJob(id string) (*core.Job, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if job, ok := j.pending[id]; ok {
		return job, nil
	}
	if job, ok := j.active[id]; ok {
		return job, nil
	}

	return nil, fmt.Errorf("job %s not found", id)
}

func (j *Jobs) PendingJobs() []*core.Job {
	j.mu.Lock()
	defer j.mu.Unlock()

	pendingJobs := make([]*core.Job, 0, len(j.pending))
	for _, job := range j.pending {
		pendingJobs = append(pendingJobs, job)
	}
	return pendingJobs
}

func (j *Jobs) ActiveJobs() []*core.Job {
	j.mu.Lock()
	defer j.mu.Unlock()

	activeJobs := make([]*core.Job, 0, len(j.active))
	for _, job := range j.active {
		activeJobs = append(activeJobs, job)
	}
	return activeJobs
}

func (j *Jobs) LenPending() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.pending)
}

func (j *Jobs) LenActive() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.active)
}

func (j *Jobs) MarkRunning(id string, containerID string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if job, ok := j.active[id]; ok {
		job.AssignedContainerID = containerID
		return nil
	}

	return fmt.Errorf("job %s not found in active state", id)
}

func (j *Jobs) MarkCompleted(id string, payload json.RawMessage, success bool) error {
	j.mu.Lock()

	job, ok := j.active[id]
	if !ok {
		j.mu.Unlock()
		return fmt.Errorf("job %s is not active", id)
	}

	delete(j.active, id)
	job.Payload = payload
	job.Succeeded = success

	j.mu.Unlock()

	j.Completed <- job
	return nil
}

func (j *Jobs) RollbackJob(id string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if job, ok := j.active[id]; ok {
		delete(j.active, id)
		job.AssignedContainerID = ""
		j.pending[id] = job
		return nil
	}

	return fmt.Errorf("job %s does not exist in active list", id)
}

func (j *Jobs) ClearAllJobs() {
	j.mu.Lock()

	// Extract to a slice so we don't hold the lock while blocking on the channel
	var toComplete []*core.Job
	for _, job := range j.active {
		toComplete = append(toComplete, job)
	}
	for _, job := range j.pending {
		toComplete = append(toComplete, job)
	}

	j.active = make(map[string]*core.Job)
	j.pending = make(map[string]*core.Job)
	j.mu.Unlock()

	for _, job := range toComplete {
		j.Completed <- job
	}
}

func (j *Jobs) StreamCompleted(stream chan<- *core.Job) {
	for job := range j.Completed {
		stream <- job
	}
}

func (j *Jobs) Close() {
	j.ClearAllJobs()
	j.mu.Lock()
	defer j.mu.Unlock()
	close(j.Completed)
}

func (j *Jobs) QueueUtilization() (int64, int64) {
	j.mu.Lock()
	defer j.mu.Unlock()

	var cpuUsage, memUsage int64
	for _, job := range j.active {
		cpuUsage += job.Image.NanoCPULimit
		memUsage += job.Image.MemoryBytesLimit
	}
	for _, job := range j.pending {
		cpuUsage += job.Image.NanoCPULimit
		memUsage += job.Image.MemoryBytesLimit
	}

	return cpuUsage, memUsage
}

func (j *Jobs) ActiveUtilization() (int64, int64) {
	j.mu.Lock()
	defer j.mu.Unlock()

	var cpuUsage, memUsage int64
	for _, job := range j.active {
		cpuUsage += job.Image.NanoCPULimit
		memUsage += job.Image.MemoryBytesLimit
	}
	return cpuUsage, memUsage
}

func (j *Jobs) NextBestJob(freeContainerImageNames []string, freeCpus int64, freeMem int64) *core.Job {
	j.mu.Lock()
	defer j.mu.Unlock()

	freeMap := make(map[string]bool, len(freeContainerImageNames))
	for _, name := range freeContainerImageNames {
		freeMap[name] = true
	}

	var bestJob *core.Job
	bestMatchesFree := false

	for _, job := range j.pending {
		if job.Image.MemoryBytesLimit > freeMem || job.Image.NanoCPULimit > freeCpus {
			continue
		}

		jobMatchesFree := freeMap[job.Image.Alias]

		if bestJob == nil {
			bestJob = job
			bestMatchesFree = jobMatchesFree
			continue
		}

		if job.Priority > bestJob.Priority {
			bestJob = job
			bestMatchesFree = jobMatchesFree
			continue
		}

		// Tiebreaker
		if job.Priority == bestJob.Priority {
			if jobMatchesFree && !bestMatchesFree {
				bestJob = job
				bestMatchesFree = true
				continue
			}

			if jobMatchesFree == bestMatchesFree {
				if job.Image.MemoryBytesLimit > bestJob.Image.MemoryBytesLimit {
					bestJob = job
					bestMatchesFree = jobMatchesFree
					continue
				}
			}
		}
	}

	if bestJob != nil {
		delete(j.pending, bestJob.Id)
		j.active[bestJob.Id] = bestJob
	}
	return bestJob
}
