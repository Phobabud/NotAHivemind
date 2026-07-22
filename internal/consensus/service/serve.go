package service

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"ClusterManager/internal/consensus/core"
	"context"

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
