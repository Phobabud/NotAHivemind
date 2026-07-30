package core

import (
	"context"
	"encoding/json"
	"time"
)

// ContainerPool defines how the scheduler interacts with the cluster's resources.
// It manages the collection of containers as a whole.
type ContainerPool interface {
	/*
		Pool Management/Lifecycle-------------------------------------------------------------------------------------------
	*/

	// Close will clean up all the containers and end the error stream
	Close(ctx context.Context) error

	// DestroyAll deletes all tracked containers (pool is cleared before deletion). This deletes the actual containers
	DestroyAll(ctx context.Context) error

	// Rebuild will find all unhealthy containers and rebuild them
	Rebuild(ctx context.Context)

	/*
		Container Management------------------------------------------------------------------------------------------------
	*/

	// Add will add a container to the pool.
	//
	// This doesn't start or create the container, so it's recommended to start the container before adding it.
	Add(targetContainer ContainerAPI) error

	// AddFromImage will create, init, then add a container to the pool. This does not start the container
	AddFromImage(image *Image) (ContainerAPI, error)

	// Remove will remove a container from the pool.
	//
	// This doesn't stop the container or delete it, it just removes it from being tracked by the pool.
	Remove(id string) error

	// TrackedLaunch will launch the container, while also linking the error stream to it
	TrackedLaunch(ctx context.Context, id string)

	/*
		Container Retrieval & Operations------------------------------------------------------------------------------------
	*/

	// Get returns a pointer to a container with the given id
	Get(id string) (ContainerAPI, error)

	// FindFreeContainer will get a free container with the selected image
	FindFreeContainer(imageName string) (ContainerAPI, error)

	// ForEach performs a specific operation on all containers
	//
	// Do not pass in one of the pool's functions in, as it'll result in deadlock
	ForEach(action func(container ContainerAPI) error) error

	/*
		Metrics-------------------------------------------------------------------------------------------------------------
	*/

	// ActiveImageNames returns a list of all images that are set up as containers
	ActiveImageNames() []string

	// AvailableImageNames returns a list of images corresponding to unoccupied containers
	AvailableImageNames(ctx context.Context) []string

	// UsedSpace will find the amount of nanocpus and memory that is already used
	UsedSpace() (nanoCpu int64, memory int64)

	// Len returns the amount of registered/tracked containers
	Len() int

	// Ping checks to see if the docker daemon is online
	Ping() bool
}

// ContainerAPI defines what the scheduler is allowed to do to an individual container.
type ContainerAPI interface {
	Id() string
	LocalPort() string
	Image() *Image

	/*
		Core Operations & Lifecycle-----------------------------------------------------------------------------------------
	*/

	// Init creates a container based on a provided container name. Does not launch container.
	Init(ctx context.Context, args ...string) (string, error)

	// Delete a container based on a provided container name.
	// Even if it's running, will delete it.
	Delete(ctx context.Context) error

	// Stop stops a container based on a provided container, with an integer timeout in seconds for graceful shutdown.
	Stop(ctx context.Context, timeout int) error

	// Rebuild rebuilds a container based on an already existing container. This doesn't launch the container.
	//
	// This does not restart a container, instead deleting the old container and "cloning" it from the same image.
	// It keeps the same var/obj, but the internals, including its Id will change.
	Rebuild(ctx context.Context, newPort string) error

	// Launch launches a container based on a provided container name. This func blocks.
	//
	// This won't create the container, since a container should be created before launch or on main thread.
	// returns a string array so that the stdout from the program can be written to a log file.
	Launch(ctx context.Context) error

	/*
		Job Assignment, State, and Handling---------------------------------------------------------------------------------
	*/

	AssignJob(jobID string, payload json.RawMessage) error
	WaitForJobResult(ctx context.Context) json.RawMessage
	FreeJob() error
	HasJob() bool
	JobID() string
	TotalJobs() int
	LastAssignedJobTime() time.Time
	LastCompletedJobTime() time.Time

	// Persistent returns a bool indicating if it's a long-running task (true), or a short-lived task (false).
	// Used in health monitoring/handling
	Persistent() bool

	/*
		Health, Status, & Usage---------------------------------------------------------------------------------------------
	*/

	// Health inspects the /healthy endpoint that docker calls to see if a container is healthy (or its overall status).
	//
	// Returns an integer and an error. The codes are: Healthy=0, Starting=1, Unhealthy=2, None=3.
	Health(ctx context.Context) (int, error)

	// Running returns a value representing if the container is running or not, as registered in the docker api
	Running(ctx context.Context) (bool, error)

	// Usage gets the current CPU and mem usage of a container, not the maximum limits.
	//
	// Returns in the order of CPU (%), Memory (Bytes), and an error.
	Usage(ctx context.Context) (float64, uint64, error)
}

// JobQueue manages the lifecycle, storage, and prioritization of all jobs.
type JobQueue interface {
	/*
		Job Management & Lifecycle------------------------------------------------------------------------------------------
	*/

	// AddJob takes a job pointer, and adds it to the pending job list in priority order. IDs must be unique
	AddJob(job *Job) error

	// RemoveJob takes a job's ID, and removes it from the pending job list
	RemoveJob(id string) error

	// FindJob takes a job's ID and finds it in the pending jobs list
	FindJob(id string) (*Job, error)

	// PendingJobs creates a snapshot of the pending jobs array (do not modify contents)
	PendingJobs() []*Job

	// ActiveJobs creates a snapshot of the active jobs array (do not modify contents)
	ActiveJobs() []*Job

	// LenPending returns the length of the pending job list
	LenPending() int

	// LenActive returns the length of the active job list
	LenActive() int

	// ClearAllJobs marks all jobs as completed, streaming everything with empty payloads
	ClearAllJobs()

	// StreamCompleted takes an input stream, and "connects" the completed job channel to the desired output channel. Run this as a goroutine
	StreamCompleted(stream chan<- *Job)

	// Close closes the Completed channel
	Close()

	/*
		Job State Management------------------------------------------------------------------------------------------------
	*/

	// MarkRunning will take a pending job and move it to be active
	MarkRunning(id string, containerID string) error

	// RollbackJob will take an active job and move it back to pending
	RollbackJob(id string) error

	// MarkCompleted removes a job from active and sends it to the completed job channel, routing it to whatever callback is desired
	MarkCompleted(id string, payload json.RawMessage, success bool) error

	/*
		Packing-------------------------------------------------------------------------------------------------------------
	*/

	// NextBestJob will attempt to find the next best job based on the following heuristic
	//
	// If the highest priority group (IE all jobs at priority 10, where 10 is the highest) has a container free to do one of the jobs in that group, it'll return that job.
	// If no job in the highest priority group could be found, it'll find the largest possible image that a job in the high priority group needs that can fit
	// If no jobs in that high priority group can fit, it'll move to the next group
	// If no jobs can be packed, it'll return nil, indicating to the scheduler that the existing containers need to be torn down
	//
	// freeContainerImageNames is a list of strings that represent the containers (by image name) that are free
	// freeCpus represents the amount of free nano cpus
	// freeMem represents the amount of free memory
	NextBestJob(freeContainers []string, freeCpu int64, freeMem int64) *Job

	// NextLargestJob will attempt to find a pending job large enough to fill the free resources, based on priority order
	NextLargestJob(freeCpu int64, freeMem int64) *Job
}

// LogStreamer handles asynchronous logging for containers.
// This allows the container package to stream logs without importing the fs package.
type LogStreamer interface {
	Add(entry []string, filename string)
	FreeFile(filename string)
	Close()
}

// ConfigLoader is responsible for retrieving the physical limits of the machine.
type ConfigLoader interface {
	GetMachineLimits() (maxCPUs int64, maxMemory int64)
	GetImages() []*Image
}
