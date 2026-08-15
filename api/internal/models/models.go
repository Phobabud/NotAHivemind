package models

import "encoding/json"

type JobPayload struct {
	ID                string          `json:"id"`
	State             Status          `json:"state"`
	Image             string          `json:"image"`
	Priority          int             `json:"priority"`
	CPURequirement    int64           `json:"cpu_requirement"`
	MemoryRequirement int64           `json:"memory_requirement"`
	Payload           json.RawMessage `json:"payload"`
}

type Status int

const (
	Pending Status = iota
	Running
	Completed
	Failed
)

func (s Status) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Running:
		return "Running"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// SchedulerPayload is a lightweight payload used for heartbeat logging and cluster registration.
type SchedulerPayload struct {
	ID                string `json:"id"`
	OriginSchedulerID string `json:"scheduler"`
	Entry             string `json:"entry"`     // E.g., "ONLINE", "HEARTBEAT", "OFFLINE"
	Timestamp         int64  `json:"timestamp"` // Unix Millis
}

type SchedulerEvent int

const (
	Online SchedulerEvent = iota
	Offline
	HeartbeatFailed   // If another scheduler cannot reach this scheduler, it will log a HeartbeatFailed event.
	RedistributedJobs // If the scheduler fails to respond with online to a HeartbeatFailed, another scheduler will log a RedistributedJobs
)

func (s SchedulerEvent) String() string {
	switch s {
	case Online:
		return "Online"
	case Offline:
		return "Offline"
	case HeartbeatFailed:
		return "HeartbeatFailed"
	case RedistributedJobs:
		return "RedistributedJobs"
	default:
		return "Unknown"
	}
}
