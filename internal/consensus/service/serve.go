package service

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"ClusterManager/internal/consensus/core"
	"ClusterManager/internal/models"
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/glog"
)

func (n *Node) ClientAppend(ctx context.Context, req *pb.RawAppendRequest) (*pb.AppendResponse, error) {
	leaderAddr := ""
	role := n.state.Role()

	// Only return a leader redirect address if we are actively a Follower.
	if role != core.Leader {
		leaderID := n.state.LeaderID()
		if leaderID != "" {
			if leaderID == n.state.ID() {
				leaderAddr = n.state.Address()
			} else if peer, ok := n.peerNodes[leaderID]; ok && peer != nil {
				leaderAddr = peer.Address()
			}
		}
	}

	stdResp := &pb.AppendResponse{
		RequestId:     req.RequestId,
		Status:        2,
		PrevLogTerm:   -1,
		PrevLogIndex:  -1,
		LeaderAddress: leaderAddr,
	}

	if role != core.Leader {
		return stdResp, nil
	}

	// Delegate to the helper to check for Thundering Herd race conditions
	if n.checkCASConflict(req.Payload) {
		stdResp.Status = 4 // 4 = Conflict
		return stdResp, nil
	}

	select {
	case n.appendChan <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	ch := n.reqRegistry.Register(req.RequestId)
	select {
	case <-ctx.Done():
		return stdResp, nil
	case resp := <-ch:
		if resp {
			stdResp.Status = 1
		} else {
			// Return Status 3 (Consensus Failure) if replication failed to achieve quorum.
			stdResp.Status = 3
		}
		return stdResp, nil
	}
}

// checkCASConflict inspects the incoming payload against the current authoritative log to prevent duplicates
func (n *Node) checkCASConflict(payload []byte) bool {
	type peekPayload struct {
		ID        string `json:"id"`
		State     *int   `json:"state"`
		Entry     any    `json:"entry"`
		Timestamp int64  `json:"timestamp"`
	}

	var incoming peekPayload
	if err := json.Unmarshal(payload, &incoming); err != nil || incoming.ID == "" {
		return false // Not a parsable or tracked payload, let it through
	}

	// Only perform the log scan if we are attempting a potentially conflicting state change
	isJobClaim := incoming.State != nil && *incoming.State >= 0
	isSchedulerEvent := incoming.Entry != nil

	if !isJobClaim && !isSchedulerEvent {
		return false // Nothing to guard against
	}

	existing := n.FileHandler.QueryEntries(nil, &incoming.ID)
	if len(existing) == 0 {
		return false
	}

	var current peekPayload
	if err := json.Unmarshal(existing[0].Payload, &current); err != nil {
		return false
	}

	// Guard for Job Protection
	if isJobClaim && current.State != nil && *current.State >= 0 && *current.State == *incoming.State {
		glog.V(3).Infof("ClientAppend: Rejecting concurrent claim for job %s.", incoming.ID)
		return true
	}

	// Guard for Scheduler Eviction Events
	if isSchedulerEvent && current.Entry != nil {
		inStr := fmt.Sprintf("%v", incoming.Entry)
		curStr := fmt.Sprintf("%v", current.Entry)

		// Target events that trigger cascading actions and shouldn't be duplicated rapidly
		isThunderingHerdEvent := inStr == models.HeartbeatFailed.String() || inStr == models.RedistributedJobs.String() || inStr == "2" || inStr == "3"

		if isThunderingHerdEvent && inStr == curStr {
			timeDiffMillis := incoming.Timestamp - current.Timestamp

			// Prevent consecutive identical writes within 5 seconds (5000 ms)
			if timeDiffMillis > -5000 && timeDiffMillis < 5000 {
				glog.V(3).Infof("ClientAppend: Rejecting duplicate scheduler event '%s' for %s (rate limit).", inStr, incoming.ID)
				return true
			}
		}
	}

	return false
}

// GetData retrieves the latest state of a specific payload from the consensus log.
// Any cluster (Leader or Follower) can serve this read request, allowing for read-scaling.
func (n *Node) GetData(ctx context.Context, req *pb.GetDataRequest) (*pb.GetDataResponse, error) {
	glog.V(2).Infof("GetData: Received query for SchedulerID: %v, PayloadID: %v", req.SchedulerIdQuery, req.PayloadIdQuery)

	// Query the local filesystem/memory handler based on optional filters
	entries := n.FileHandler.QueryEntries(req.SchedulerIdQuery, req.PayloadIdQuery)

	resp := &pb.GetDataResponse{
		OriginNodeId: n.state.ID(),
		Exists:       len(entries) > 0,
	}

	// Pack all matched results into the response array
	for _, entry := range entries {
		resp.Payloads = append(resp.Payloads, entry.Payload)
	}

	return resp, nil
}

// InstallSnapshot receives chunked binary blocks from the Leader and mounts them onto local storage.
func (n *Node) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	resp := &pb.InstallSnapshotResponse{
		Term: n.state.CurrentTerm(),
	}

	if req.Term < n.state.CurrentTerm() {
		glog.V(2).Infof("InstallSnapshot: Rejecting snapshot from %s: Term %d < our Term %d", req.LeaderId, req.Term, n.state.CurrentTerm())
		return resp, nil
	}

	if req.Term > n.state.CurrentTerm() {
		n.state.SetCurrentTerm(req.Term)
		n.state.SetVotedFor("")
	}

	n.state.RecordContact()
	n.state.UpdateLeader(req.LeaderId)
	if n.state.Role() == core.Candidate || n.state.Role() == core.Leader {
		n.state.BecomeFollower()
	}

	err := n.FileHandler.InstallSnapshot(req.Offset, req.Data, req.Done, req.LastIncludedIndex, req.LastIncludedTerm)
	if err != nil {
		glog.Errorf("InstallSnapshot: Failed to write snapshot chunk at offset %d: %v", req.Offset, err)
		return nil, err
	}

	return resp, nil
}
