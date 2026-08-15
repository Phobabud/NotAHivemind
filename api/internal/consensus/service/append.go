package service

import (
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/consensus/filesystem"
	"context"
	"errors"

	pb "NotAHiveMind/api/gen/consensus/v1"

	"github.com/golang/glog"
)

// AppendEntries handles incoming log replication and heartbeats from the Leader.
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	leaderAddr := ""
	leaderID := n.state.LeaderID()

	if leaderID != "" {
		if leaderID == n.state.ID() {
			leaderAddr = n.state.Address()
		} else if peer, ok := n.peerNodes[leaderID]; ok && peer != nil {
			leaderAddr = peer.Address()
		}
	}

	defaultResponse := &pb.AppendResponse{
		RequestId:     req.RequestId,
		PrevLogTerm:   n.state.CurrentTerm(), // Follower returns its logical term to sync the leader
		PrevLogIndex:  n.FileHandler.Index(),
		LeaderAddress: leaderAddr,
	}

	// Compare incoming heartbeat terms against the cluster's current state machine term
	if req.Term < n.state.CurrentTerm() {
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD
		glog.V(2).Infof("Received AppendEntries from %s for Term %d, but our term is %d. Rejecting.", req.OriginNodeId, req.Term, n.state.CurrentTerm())
		return defaultResponse, nil
	}

	if req.Term > n.state.CurrentTerm() {
		n.state.SetCurrentTerm(req.Term)
	}

	n.state.RecordContact()
	n.state.UpdateLeader(req.OriginNodeId)
	n.state.SetVotedFor("") // Clear voting records on a verified higher-epoch entry

	if n.state.Role() == core.Candidate || n.state.Role() == core.Leader {
		n.state.BecomeFollower()
	}

	if req.PrevLogIndex > n.FileHandler.Index() {
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD
		glog.V(2).Infof("Received AppendEntries from %s for Index %d, but our log index is %d. Rejecting.", req.OriginNodeId, req.Index, n.FileHandler.Index())
		return defaultResponse, nil
	}

	// Prev log term validation check
	if req.PrevLogIndex == n.FileHandler.Index() && req.PrevLogTerm != n.FileHandler.Term() {
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD
		glog.V(2).Infof("Received AppendEntries from %s with PrevLogTerm %d, but our log term at index %d is %d. Rejecting.", req.OriginNodeId, req.PrevLogTerm, req.PrevLogIndex, n.FileHandler.Term())
		return defaultResponse, nil
	}

	isHeartbeat := len(req.Payload) == 0
	if isHeartbeat {
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS
		return defaultResponse, nil
	}

	// Append log entry
	data := filesystem.LogEntry{
		Index:       req.Index,
		Term:        req.Term,
		SchedulerID: req.SchedulerId,
		Payload:     req.Payload,
	}
	err := n.FileHandler.Append(&data)
	switch {
	case err == nil:
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS
		return defaultResponse, nil
	case errors.Is(err, core.ErrAppendTermBehind), errors.Is(err, core.ErrAppendIndexBehind):
		diskTerm := n.FileHandler.DiscTerm()
		diskIndex := n.FileHandler.DiscIndex()
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD
		if data.Term < diskTerm || data.Index <= diskIndex {
			defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_NEEDS_SNAPSHOT
			return defaultResponse, nil
		}
		if err := n.FileHandler.TruncateLog(data.Index); err != nil {
			return defaultResponse, nil
		}
		if err := n.FileHandler.Append(&data); err != nil {
			return defaultResponse, nil
		}
	default:
		defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD
		return defaultResponse, nil
	}

	glog.V(2).Infof("Received AppendEntries from Leader for Term %d", req.Term)
	defaultResponse.Status = pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS
	return defaultResponse, nil
}
