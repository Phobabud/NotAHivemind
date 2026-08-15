package consensus

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/models"
	"NotAHiveMind/internal/scheduler/core"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// commitPayload handles the generic gRPC call and Raft status evaluation.
// It fails fast and returns an error if consensus is not immediately available.
func (c *RaftClient) commitPayload(ctx context.Context, schedulerID string, reqID string, payload []byte, logContext string) error {
	req := &pb.RawAppendRequest{
		RequestId:   reqID,
		SchedulerId: schedulerID,
		Payload:     payload,
	}

	// Thread-safely snapshot the current connection/client
	c.mu.RLock()
	client := c.client
	addr := c.address
	c.mu.RUnlock()

	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	resp, err := client.ClientAppend(rpcCtx, req)
	cancel()

	if err != nil {
		return fmt.Errorf("raft append failed for %s on cluster %s (network failure): %w", logContext, addr, err)
	}

	if resp.Status != pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS && resp.LeaderAddress != c.address && resp.LeaderAddress != "" {
		if err := c.switchConnection(resp.LeaderAddress); err != nil {
			return err
		}
	}

	switch resp.Status {
	case pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS:
		return nil
	case pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD:
		return fmt.Errorf("bad payload")
	case pb.AppendResponseStatus_APPEND_RESPONSE_BAD_ORIGIN_ID:
		return fmt.Errorf("bad origin ID")
	case pb.AppendResponseStatus_APPEND_RESPONSE_FAILED_CONSENSUS:
		return fmt.Errorf("failed to reach quorum")
	case pb.AppendResponseStatus_APPEND_RESPONSE_CONCURRENT_CLAIM:
		return core.ErrCASConcurrentConflict
	default:
		return fmt.Errorf("unknown Raft response status: %v", resp.Status)
	}
}

// FetchData queries the Raft consensus group for the latest raw state.
func (c *RaftClient) FetchData(ctx context.Context, originNodeID string, schedulerIDQuery *string, payloadIDQuery *string) ([][]byte, bool, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	req := &pb.GetDataRequest{
		OriginNodeId:     originNodeID,
		SchedulerIdQuery: schedulerIDQuery,
		PayloadIdQuery:   payloadIDQuery,
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := client.GetData(rpcCtx, req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to fetch data from Raft cluster: %w", err)
	}

	return resp.Payloads, resp.Exists, nil
}

// FetchJob retrieves the most recent job status directly from the raft engine, interpreting the payload.
func (c *RaftClient) FetchJob(ctx context.Context, originNodeID string, jobID string) (core.Job, error) {
	data, ok, err := c.FetchData(ctx, originNodeID, nil, &jobID)
	if err != nil {
		return core.Job{}, err
	}
	if !ok || len(data) == 0 {
		return core.Job{}, fmt.Errorf("failed to fetch job %s from Raft: not found", jobID)
	}

	var job models.JobPayload
	var schedulerJob core.Job
	if err := json.Unmarshal(data[0], &job); err != nil {
		return core.Job{}, fmt.Errorf("failed to parse job %s data: %w", jobID, err)
	}

	schedulerJob.CastFromGlobalModel(job)
	return schedulerJob, nil
}

// CommitJob acts as the main entry point for the Scheduler to persist state.
func (c *RaftClient) CommitJob(ctx context.Context, schedulerID string, job *core.Job) error {
	payloadObj := models.JobPayload{
		ID:       job.Id,
		State:    job.Status,
		Image:    job.ImageAlias,
		Priority: job.Priority,
		Payload:  job.Payload,
	}

	payload, err := json.Marshal(payloadObj)
	if err != nil {
		return fmt.Errorf("failed to serialize job %s for Raft: %w", job.Id, err)
	}

	reqID := models.NewJobID()
	logContext := fmt.Sprintf("job %s", job.Id)

	if err := c.commitPayload(ctx, schedulerID, reqID.String(), payload, logContext); err != nil {
		return err
	}

	_, ok, err := c.FetchData(ctx, schedulerID, &schedulerID, &job.Id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to verify committed job %s from Raft", job.Id)
	}

	return nil
}

// FetchSchedulerEvent fetches and interprets the most recent scheduler event from Raft.
func (c *RaftClient) FetchSchedulerEvent(ctx context.Context, originNodeID string, schedulerID string) (models.SchedulerPayload, error) {
	data, ok, err := c.FetchData(ctx, originNodeID, nil, &schedulerID)
	if err != nil {
		return models.SchedulerPayload{}, err
	}
	if !ok || len(data) == 0 {
		return models.SchedulerPayload{}, fmt.Errorf("failed to fetch scheduler event %s from Raft: not found", schedulerID)
	}

	var eventPayload models.SchedulerPayload
	if err := json.Unmarshal(data[0], &eventPayload); err != nil {
		return models.SchedulerPayload{}, fmt.Errorf("failed to parse scheduler event data: %w", err)
	}

	if eventPayload.ID != schedulerID {
		return models.SchedulerPayload{}, fmt.Errorf("failed to declare scheduler event--another scheduler already did it: %s", eventPayload.ID)
	}

	return eventPayload, nil
}

// CommitSchedulerEvent writes a scheduler lifecycle event to Raft and verifies it.
func (c *RaftClient) CommitSchedulerEvent(ctx context.Context, originSchedulerID string, schedulerID string, event string) error {
	payloadObj := models.SchedulerPayload{
		ID:                schedulerID,
		OriginSchedulerID: originSchedulerID, // Included based on the struct definition
		Entry:             event,
		Timestamp:         time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(payloadObj)
	if err != nil {
		return fmt.Errorf("failed to serialize scheduler event for Raft: %w", err)
	}

	reqID := models.NewRequestID()
	logContext := fmt.Sprintf("scheduler event '%s'", event)

	if err := c.commitPayload(ctx, schedulerID, reqID.String(), payload, logContext); err != nil {
		return err
	}

	_, err = c.FetchSchedulerEvent(ctx, originSchedulerID, schedulerID)
	return err
}
