package core

import (
	"encoding/json"
)

type Job struct {
	Id                  string
	Image               *Image
	AssignedContainerID string
	Priority            int
	Payload             json.RawMessage
	Response            json.RawMessage
	Succeeded           bool
}

// NewJob creates a new Job instance and returns a pointer to it
func NewJob(id string, image *Image, priority int, payload json.RawMessage) *Job {
	job := &Job{
		Id:                  id,
		Image:               image,
		AssignedContainerID: "",
		Priority:            priority,
		Payload:             payload,
		Response:            nil,
	}
	return job
}

type Image struct {
	Alias       string   `json:"alias"`
	ImageName   string   `json:"image_name"`
	Volumes     []Volume `json:"volumes"`
	Args        []string `json:"args"`
	CPULimit    int64    `json:"CPULimit"`    // Bytes, 0 for unlimited
	MemoryLimit int64    `json:"memoryLimit"` // 1 CPU = 1,000,000,000 NanoCPUs, unit is in NanoCPUs
	Persistent  bool     `json:"persistent"`  // Does the program run continuously (true, no monitoring for results), or handles specific jobs (false)
	MaxJobs     int      `json:"max_jobs"`    // How many jobs can the program run before being recycled?
	Timeout     int      `json:"timeout"`     // In seconds. Ignored if persistent is true
}

type Volume struct {
	Type   string `json:"type"`
	Source string `json:"source"`
	Target string `json:"target"`
}
