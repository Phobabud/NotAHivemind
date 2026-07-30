package service

import (
	"ClusterManager/internal/consensus/core"
	"context"
	"sync"
	"time"

	"github.com/golang/glog"

	pb "ClusterManager/api/gen/consensus/v1"
)

// RequestVote handles incoming vote requests from other Candidates.
// This implements the server-side of the ConsensusCoordinationService.
func (n *Node) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if req.Term > n.state.CurrentTerm() {
		if req.Type == 1 { // Standard Vote
			glog.V(1).Infof("RequestVote: Stepping down to follower because Term %d > our Term %d", req.Term, n.state.CurrentTerm())
			n.state.SetVotedFor("")
			n.state.BecomeFollower()
			n.state.SetCurrentTerm(req.Term)
		}
	}

	if req.Term < n.state.CurrentTerm() {
		glog.V(2).Infof("Received RequestVote from %s for Term %d (Term Behind)", req.OriginNodeId, req.Term)
		return &pb.VoteResponse{
			ResponseNodeId: n.state.ID(),
			Vote:           false,
		}, nil
	}

	if req.Type == 1 && (n.state.Role() == core.Leader || n.state.Role() == core.Candidate) && req.Term == n.state.CurrentTerm() {
		glog.V(2).Infof("RequestVote term %v; ignoring", req.Term)
		return &pb.VoteResponse{
			ResponseNodeId: n.state.ID(),
			Vote:           false,
		}, nil
	}

	if req.Type == 2 && n.state.LeaderID() != "" && !n.state.IsElectionTimeout() {
		glog.V(2).Infof("Rejecting Pre-Vote request from %s for Term %d due to active leader lease.", req.OriginNodeId, req.Term)
		return &pb.VoteResponse{
			ResponseNodeId: n.state.ID(),
			Vote:           false,
		}, nil
	}

	lastLogTerm := n.FileHandler.Term()
	lastLogIndex := n.FileHandler.Index()
	logIsUpToDate := req.LogTerm > lastLogTerm || (req.LogTerm == lastLogTerm && req.LogIndex >= lastLogIndex)

	votedFor := n.state.VotedFor()

	if req.Type == 2 {
		if logIsUpToDate {
			glog.V(2).Infof("Granting tentative Pre-Vote to candidate %s for hypothetical Term %d", req.OriginNodeId, req.Term)
			return &pb.VoteResponse{
				ResponseNodeId: n.state.ID(),
				Vote:           true,
			}, nil
		}
		return &pb.VoteResponse{
			ResponseNodeId: n.state.ID(),
			Vote:           false,
		}, nil
	}

	if (votedFor == "" || votedFor == req.OriginNodeId) && logIsUpToDate {
		n.state.SetVotedFor(req.OriginNodeId)
		n.state.UpdateLeader("")
		n.state.RecordContact()

		glog.V(2).Infof("Granting formal Vote to candidate %s for Term %d", req.OriginNodeId, req.Term)
		return &pb.VoteResponse{
			ResponseNodeId: n.state.ID(),
			Vote:           true,
		}, nil
	}

	glog.V(2).Infof("Received RequestVote from %s for Term %d (Vote Rejected)", req.OriginNodeId, req.Term)
	return &pb.VoteResponse{
		ResponseNodeId: n.state.ID(),
		Vote:           false,
	}, nil
}

// StartElection triggers leader transitions and sends out request votes.
func (n *Node) StartElection(ctx context.Context) {
	n.mutex.Lock()

	if n.state.Role() == core.Leader {
		n.mutex.Unlock()
		return
	}

	n.state.UpdateLeader("")
	n.state.RecordContact()

	oldTerm := n.state.CurrentTerm()
	preVoteTerm := oldTerm + 1

	preVoteReq := &pb.VoteRequest{
		Term:         preVoteTerm,
		OriginNodeId: n.state.ID(),
		LogIndex:     n.FileHandler.Index(),
		LogTerm:      n.FileHandler.Term(),
		Type:         2, // PreVote
	}

	n.mutex.Unlock()
	peers := n.Peers()

	glog.V(2).Infof("Node %s initiating tentative Pre-Vote phase for Term %d", n.state.ID(), preVoteTerm)

	majorityNeeded := (len(peers)+1)/2 + 1

	if majorityNeeded == 1 {
		n.mutex.Lock()
		if n.state.CurrentTerm() == oldTerm {
			glog.V(1).Infof("Node %s won election (single cluster cluster)", n.state.ID())
			n.state.BecomeCandidate()
			n.state.SetCurrentTerm(preVoteTerm)
			n.state.SetVotedFor(n.state.ID())
			n.state.BecomeLeader()
			n.state.UpdateLeader(n.state.ID())
			n.commitIndex = n.FileHandler.Index() // Process any backlogs
			go n.BroadcastHeartbeats(ctx)
		}
		n.mutex.Unlock()
		return
	}

	preVoteSuccess := n.sendVoteRequests(ctx, preVoteReq, peers, majorityNeeded, core.Follower, oldTerm)
	if !preVoteSuccess {
		glog.V(1).Infof("Node %s failed to secure Pre-Vote majority. Aborting election sequence.", n.state.ID())
		return
	}

	glog.V(1).Infof("Node %s successfully won Pre-Vote phase. Launching formal election for Term %d.", n.state.ID(), preVoteTerm)

	n.mutex.Lock()
	if n.state.Role() == core.Leader || n.state.CurrentTerm() != oldTerm {
		n.mutex.Unlock()
		return
	}

	n.state.BecomeCandidate()
	n.state.SetCurrentTerm(preVoteTerm)
	n.state.SetVotedFor(n.state.ID())

	req := &pb.VoteRequest{
		Term:         preVoteTerm,
		OriginNodeId: n.state.ID(),
		LogIndex:     n.FileHandler.Index(),
		LogTerm:      n.FileHandler.Term(),
		Type:         1,
	}
	n.mutex.Unlock()

	electionSuccess := n.sendVoteRequests(ctx, req, peers, majorityNeeded, core.Candidate, preVoteTerm)
	if electionSuccess {
		n.mutex.Lock()
		if n.state.Role() == core.Candidate && n.state.CurrentTerm() == preVoteTerm {
			glog.V(1).Infof("Node %s won election for Term %d", n.state.ID(), preVoteTerm)
			n.state.BecomeLeader()
			n.state.UpdateLeader(n.state.ID())
			n.commitIndex = n.FileHandler.Index() // Ensure state transitions process safely
			go n.BroadcastHeartbeats(ctx)
		}
		n.mutex.Unlock()
	}
}

func (n *Node) sendVoteRequests(ctx context.Context, payload *pb.VoteRequest, peers []core.PeerState, majorityNeeded int, expectedRole core.Role, expectedTerm int64) bool {
	var wg sync.WaitGroup
	var votesMu sync.Mutex
	votes := 1

	for _, peer := range peers {
		if !peer.Connection() {
			continue
		}

		wg.Add(1)

		go func(p core.PeerState) {
			defer wg.Done()

			rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			resp, err := p.Client().RequestVote(rpcCtx, payload)
			if err != nil {
				glog.V(2).Infof("Failed to send RequestVote to %s: %v", p.ID(), err)
				return
			}

			n.mutex.Lock()
			currentRole := n.state.Role()
			currentTerm := n.state.CurrentTerm()
			n.mutex.Unlock()

			if currentRole != expectedRole || currentTerm != expectedTerm {
				return
			}

			if resp.Vote {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}(peer)
	}

	wg.Wait()
	return votes >= majorityNeeded
}
