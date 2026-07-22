package cluster

import (
	pb "ClusterManager/api/gen/cluster/v1"
	"time"
)

// Status represents the internal, clean state of a worker node.
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
	Status     string
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
		TotalCPU:        int(resp.TotalCpu),
		TotalMemory:     int(resp.TotalMemory),
		AvailableCPU:    int(resp.AvailableCpu),
		AvailableMemory: int(resp.AvailableMemory),
		ActiveJobIDs:    resp.ActiveJobIds,
		LastUpdated:     time.Now(),
	}
}

// ParseJobEvent translates the incoming stream response into an internal event.
func ParseJobEvent(resp *pb.JobStatusResponse) *JobEvent {
	if resp == nil {
		return nil
	}

	event := &JobEvent{
		JobID:      resp.JobId,
		Status:     resp.Status,
		Priority:   int(resp.Priority),
		ImageAlias: resp.ImageAlias,
		//TODO: check success value handling
	}

	if resp.Payload != nil {
		event.Payload = resp.Payload
	}

	return event
}
