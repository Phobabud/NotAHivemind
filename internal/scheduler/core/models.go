package core

import (
	"ClusterManager/internal/models"
)

type Job struct {
	Id                string
	ImageAlias        string
	AssignedClusterId string
	Status            models.Status
	CPURequirement    int64
	MemoryRequirement int64
	Priority          int
	Payload           []byte
	Response          []byte
}

func (j *Job) CastFromGlobalModel(job models.JobPayload) {
	j.Id = job.ID
	j.ImageAlias = job.Image
	j.AssignedClusterId = ""
	j.Status = job.State
	j.CPURequirement = job.CPURequirement
	j.MemoryRequirement = job.MemoryRequirement
	j.Priority = job.Priority
	j.Payload = job.Payload
	j.Response = nil
}
