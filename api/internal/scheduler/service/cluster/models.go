package cluster

import (
	pb "NotAHiveMind/api/gen/cluster/v1"
	"NotAHiveMind/internal/models"
	"time"
)

// Status represents the internal, clean state of a worker cluster.
type Status struct {
	NodeID          string
	Address         string
	TotalCPU        int
	TotalMemory     int
	AvailableCPU    int
	AvailableMemory int
	ActiveJobIDs    []string
	LastUpdated     time.Time
}

// JobEvent represents a terminal state transition from the worker.
type JobEvent struct {
	JobID      string
	Status     models.Status
	Priority   int
	ImageAlias string
	Payload    []byte
	Success    bool
}

// ParseClusterStatus acts as an Anti-Corruption Layer, translating the gRPC
// wire-format object into our clean internal domain model.
func ParseClusterStatus(resp *pb.ClusterStatusResponse) *Status {
	if resp == nil {
		return nil
	}

	return &Status{
		NodeID:          resp.NodeId,
		Address:         resp.NodeAddress,
		TotalCPU:        int(resp.TotalNanoCpu),
		TotalMemory:     int(resp.TotalMemoryBytes),
		AvailableCPU:    int(resp.AvailableNanoCpu),
		AvailableMemory: int(resp.AvailableMemoryBytes),
		ActiveJobIDs:    resp.ActiveJobIds,
		LastUpdated:     time.Now(),
	}
}

// ParseJobEvent translates the incoming stream response into an internal event.
func ParseJobEvent(resp *pb.JobStatusResponse) *JobEvent {
	if resp == nil {
		return nil
	}

	var status models.Status
	switch resp.Status {
	case pb.JobStatus_JOB_STATUS_PENDING:
		status = models.Pending
	case pb.JobStatus_JOB_STATUS_RUNNING:
		status = models.Running
	case pb.JobStatus_JOB_STATUS_COMPLETED:
		status = models.Completed
	case pb.JobStatus_JOB_STATUS_FAILED:
		status = models.Failed
	default:
		status = models.Failed
	}

	event := &JobEvent{
		JobID:      resp.JobId,
		Status:     status,
		Priority:   int(resp.Priority),
		ImageAlias: resp.ImageAlias,
	}

	if resp.Payload != nil {
		event.Payload = resp.Payload
	}

	return event
}
