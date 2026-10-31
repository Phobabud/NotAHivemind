package scheduler

import (
	"NotAHiveMind/internal/arbiter"
	"NotAHiveMind/internal/cluster/core"
	"encoding/json"
	"fmt"
	"sync"
)

// Jobs implements core.JobQueue using highly optimized O(1) maps.
type Jobs struct {
	pending *arbiter.Queue[*core.Job]
	active  map[string]*core.Job

	Completed chan *core.Job
	mu        *sync.Mutex
}

func CreateJobsQueue() *Jobs {
	return &Jobs{
		pending: arbiter.NewQueue[*core.Job](
			func(j *core.Job) string { return j.Id },
			func(j *core.Job) int64 { return int64(j.Priority) },
			func(j *core.Job) int64 { return j.Image.NanoCPULimit },
			func(j *core.Job) int64 { return j.Image.MemoryBytesLimit }),
		active:    make(map[string]*core.Job),
		Completed: make(chan *core.Job, 1000),
		mu:        new(sync.Mutex),
	}
}

func (j *Jobs) AddJob(job *core.Job) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, exists := j.active[job.Id]; exists {
		return core.ErrAlreadyRunning
	}

	j.pending.Push(job) // If the job ID exists, it updates the data
	return nil
}

func (j *Jobs) RemoveJob(id string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if _, ok := j.active[id]; ok {
		return fmt.Errorf("job %s is already running and cannot be removed", id)
	}
	j.pending.RemoveByID(id)

	return nil
}

func (j *Jobs) FindJob(id string) (*core.Job, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if jobs := j.pending.PeekForHeuristic(func(job *core.Job) bool { return job.Id == id }); len(jobs) > 0 {
		return jobs[0], nil
	}
	if job, ok := j.active[id]; ok {
		return job, nil
	}

	return nil, fmt.Errorf("job %s not found", id)
}

func (j *Jobs) PendingJobs() []*core.Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.pending.Items()
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

func (j *Jobs) LenPending() int32 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.pending.Size()
}

func (j *Jobs) LenActive() int32 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return int32(len(j.active))
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
		j.pending.Push(job)
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
	for _, job := range j.pending.Items() {
		toComplete = append(toComplete, job)
	}

	j.active = make(map[string]*core.Job)
	j.pending.Clear()
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
	for _, job := range j.pending.Items() {
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

func (j *Jobs) PendingJobTypes() map[string]int {
	j.mu.Lock()
	defer j.mu.Unlock()
	jobTypes := make(map[string]int)
	for _, job := range j.pending.Items() {
		jobTypes[job.Image.Alias]++
	}
	return jobTypes
}

func (j *Jobs) NextBestJob(freeContainerImageNames []string, freeCpus int64, freeMem int64) *core.Job {
	j.mu.Lock()
	defer j.mu.Unlock()

	freeMap := make(map[string]bool, len(freeContainerImageNames))
	for _, name := range freeContainerImageNames {
		freeMap[name] = true
	}

	homogenousJob, foundHomo := j.pending.PopBestFitHeuristic(freeCpus, freeMem, func(job *core.Job) bool {
		return freeMap[job.Image.Alias]
	})

	highestPriorityJob, foundHigh := j.pending.PopBestFit(freeCpus, freeMem)

	if foundHigh {
		// If highest priority beats or equals homogenous, return highest, put homogenous back
		if !foundHomo || highestPriorityJob.Priority > homogenousJob.Priority {
			if foundHomo {
				j.pending.Push(homogenousJob)
			}

			// Directly transition to active state under the existing lock
			highestPriorityJob.AssignedContainerID = "packing-in-progress"
			j.active[highestPriorityJob.Id] = highestPriorityJob
			return highestPriorityJob
		}

		j.pending.Push(highestPriorityJob)
	}

	if foundHomo {
		// Directly transition to active state under the existing lock
		homogenousJob.AssignedContainerID = "packing-in-progress"
		j.active[homogenousJob.Id] = homogenousJob
		return homogenousJob
	}

	return nil
}
