package core

type Job struct {
	Id                string
	ImageAlias        string
	AssignedClusterId string
	Status            JobStatus
	CPULimit          int64
	MemoryLimit       int64
	Priority          int
	Payload           []byte
	Response          []byte
}

type JobStatus int

const (
	Pending JobStatus = iota
	Running
	Completed
	Failed
)
