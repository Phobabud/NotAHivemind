package scheduler

import (
	"ClusterManager/internal/cluster/core"
	"encoding/json"
	"fmt"
	"sync"
)

// Jobs represents the collection of jobs, both completed and pending jobs
type Jobs struct {
	pending   []*core.Job
	active    []*core.Job
	Completed chan *core.Job
	mutex     *sync.Mutex
}

func CreateJobsQueue() *Jobs {
	jobs := &Jobs{
		pending:   make([]*core.Job, 0),
		active:    make([]*core.Job, 0),
		Completed: make(chan *core.Job, 100),
		mutex:     new(sync.Mutex),
	}
	return jobs
}

func (j *Jobs) AddJob(job *core.Job) error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	// Ensure a job with the same ID doesn't exist.
	for _, existing := range j.pending {
		if existing.Id == job.Id {
			return fmt.Errorf("job with ID %s already exists in pending", job.Id)
		}
	}
	for _, existing := range j.active {
		if existing.Id == job.Id {
			return fmt.Errorf("job with ID %s already exists in active", job.Id)
		}
	}

	// Higher Priority first, then Larger Image Size
	insertIdx := len(j.pending)
	for i, existing := range j.pending {
		// If new job has higher priority, it goes before
		if job.Priority > existing.Priority {
			insertIdx = i
			break
		}

		// If priorities are equal, check image size (NanoCPUs/Memory)
		// Assuming 'image.MemoryLimit' is our proxy for "size"
		if job.Priority == existing.Priority {
			if job.Image.MemoryLimit > existing.Image.MemoryLimit {
				insertIdx = i
				break
			}
		}
	}

	j.pending = append(j.pending, nil)
	copy(j.pending[insertIdx+1:], j.pending[insertIdx:])
	j.pending[insertIdx] = job

	return nil
}

func (j *Jobs) RemoveJob(id string) error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	for i, job := range j.pending {
		if job.Id == id {
			// remove that job if it's not running
			if job.AssignedContainerID == "" {
				j.pending = append(j.pending[:i], j.pending[i+1:]...)
				return nil
			}
			return fmt.Errorf("job %s is already running and cannot be removed", id)
		}
	}
	return fmt.Errorf("job %s does not exist", id)
}

func (j *Jobs) FindJob(id string) (*core.Job, error) {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	for _, job := range j.pending {
		if job.Id == id {
			return job, nil
		}
	}
	return nil, fmt.Errorf("job %s not found", id)
}

func (j *Jobs) PendingJobs() []*core.Job {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	pendingJobs := make([]*core.Job, 0)
	for _, job := range j.pending {
		pendingJobs = append(pendingJobs, job)
	}
	return pendingJobs
}

func (j *Jobs) ActiveJobs() []*core.Job {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	activeJobs := make([]*core.Job, 0)
	for _, job := range j.active {
		activeJobs = append(activeJobs, job)
	}
	return activeJobs
}

func (j *Jobs) MarkRunning(id string, containerID string) error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	var targetJob *core.Job
	for i, job := range j.pending {
		if job.Id == id {
			targetJob = job
			j.pending = append(j.pending[:i], j.pending[i+1:]...)
			break
		}
	}

	if targetJob == nil {
		return fmt.Errorf("job %s not found in pending queue", id)
	}

	targetJob.AssignedContainerID = containerID
	j.active = append(j.active, targetJob)

	return nil
}

func (j *Jobs) RollbackJob(id string) error {
	j.mutex.Lock()

	var targetJob *core.Job
	for i, job := range j.active {
		if job.Id == id {
			targetJob = job
			j.active = append(j.active[:i], j.active[i+1:]...)
			break
		}
	}

	j.mutex.Unlock()

	if targetJob == nil {
		return fmt.Errorf("job %s does not exist in active list", id)
	}

	targetJob.AssignedContainerID = ""
	if err := j.AddJob(targetJob); err != nil {
		return err
	}

	return nil
}

func (j *Jobs) MarkCompleted(id string, payload json.RawMessage, success bool) error {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	var job *core.Job
	for i, aJob := range j.active {
		if aJob.Id == id {
			job = aJob
			j.active = append(j.active[:i], j.active[i+1:]...)
			break
		}
	}
	if job == nil {
		return fmt.Errorf("job %s is not running", id)
	}

	job.Payload = payload
	job.Succeeded = success
	j.Completed <- job
	return nil
}

func (j *Jobs) ClearAllJobs() {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	for _, job := range j.active {
		j.Completed <- job
	}
	for _, job := range j.pending {
		j.Completed <- job
	}
	// Clear Active and Pending lists
	j.active = make([]*core.Job, 0)
	j.pending = make([]*core.Job, 0)
}

func (j *Jobs) StreamCompleted(stream chan<- *core.Job) {
	for job := range j.Completed {
		stream <- job
	}
}

func (j *Jobs) Close() {
	j.ClearAllJobs()
	j.mutex.Lock()
	defer j.mutex.Unlock()
	close(j.Completed)
}

func (j *Jobs) LenPending() int {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return len(j.pending)
}

func (j *Jobs) LenActive() int {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return len(j.active)
}

func (j *Jobs) NextLargestJob(freeCpus int64, freeMem int64) *core.Job {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	for _, job := range j.pending {
		if job.Image.MemoryLimit < freeMem && job.Image.CPULimit < freeCpus {
			return job
		}
	}
	return nil
}

func (j *Jobs) NextBestJob(freeContainerImageNames []string, freeCpus int64, freeMem int64) *core.Job {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	// Kick off the recursive search
	return j.findNextRecursive(j.pending, freeContainerImageNames, freeCpus, freeMem)
}

// findNextRecursive is the internal recursive engine (assumes lock is held)
func (j *Jobs) findNextRecursive(jobs []*core.Job, freeContainers []string, freeCpus, freeMem int64) *core.Job {
	// Base Case: We ran out of jobs to check
	if len(jobs) == 0 {
		return nil
	}

	// Isolate the current group (IE all jobs with the same priority)
	currentPriority := jobs[0].Priority
	groupEnd := 0
	for groupEnd < len(jobs) && jobs[groupEnd].Priority == currentPriority {
		groupEnd++
	}

	currentGroup := jobs[:groupEnd]
	remainingJobs := jobs[groupEnd:]

	// Heuristic A: Exact match with a free container
	for _, job := range currentGroup {
		for _, freeName := range freeContainers {
			if job.Image.Alias == freeName {
				return job
			}
		}
	}

	// Heuristic B: Largest job that fits (Bin Packing)
	for _, job := range currentGroup {
		if job.Image.CPULimit <= freeCpus && job.Image.MemoryLimit <= freeMem {
			return job
		}
	}

	// Move on to next group if a job couldn't be found
	return j.findNextRecursive(remainingJobs, freeContainers, freeCpus, freeMem)
}
