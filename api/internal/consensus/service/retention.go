package service

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/consensus/core"
	"context"
	"sync"
	"time"
)

// BroadcastHeartbeats is used by the Leader to broadcast heartbeats and maintain quorum.
func (n *Node) BroadcastHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var nextIndex sync.Map
	n.initNextIndex(&nextIndex)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.dispatchHeartbeats(ctx, &nextIndex)
		}
	}
}

// initNextIndex populates the initial target indices for all known peers.
func (n *Node) initNextIndex(nextIndex *sync.Map) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	lastLogIdx := n.FileHandler.Index()
	for _, peer := range n.peerNodes {
		nextIndex.Store(peer.ID(), lastLogIdx+1)
	}
}

// dispatchHeartbeats validates leadership and spawns concurrent workers for each peer.
func (n *Node) dispatchHeartbeats(ctx context.Context, nextIndex *sync.Map) {
	n.mutex.Lock()
	if n.state.Role() != core.Leader {
		n.mutex.Unlock()
		return
	}

	currentTerm := n.state.CurrentTerm()
	leaderID := n.state.ID()
	n.mutex.Unlock()

	peers := n.Peers() // Snapshot copy under lock

	for _, peer := range peers {
		if !peer.Connection() {
			continue
		}

		go n.sendHeartbeatToPeer(ctx, peer, leaderID, currentTerm, nextIndex)
	}
}

// sendHeartbeatToPeer builds, transmits, and evaluates a single heartbeat.
func (n *Node) sendHeartbeatToPeer(ctx context.Context, peer core.PeerState, leaderID string, currentTerm int64, nextIndex *sync.Map) {
	n.mutex.Lock()
	if n.state.Role() != core.Leader || n.state.CurrentTerm() != currentTerm {
		n.mutex.Unlock()
		return
	}
	n.mutex.Unlock()

	val, _ := nextIndex.Load(peer.ID())
	ni := val.(int64)

	req, hasPayload := n.buildHeartbeatRequest(leaderID, currentTerm, ni)

	rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	resp, err := peer.Client().AppendEntries(rpcCtx, req)
	cancel()

	if err != nil {
		return
	}

	n.processHeartbeatResponse(peer, resp, ni, hasPayload, nextIndex)
}

// buildHeartbeatRequest fetches necessary log data and constructs the gRPC payload.
func (n *Node) buildHeartbeatRequest(leaderID string, currentTerm int64, ni int64) (*pb.AppendRequest, bool) {
	prevLogIndex := ni - 1
	var prevLogTerm int64 = 0

	if prevLogIndex == n.FileHandler.DiscIndex() {
		prevLogTerm = n.FileHandler.DiscTerm()
	}

	entries := n.FileHandler.Entries(prevLogIndex, ni+1)

	var payload []byte = nil
	var reqIndex int64 = 0
	var reqSchedulerID = ""

	for _, entry := range entries {
		if entry.Index == prevLogIndex {
			prevLogTerm = entry.Term
		}
		if entry.Index == ni {
			payload = entry.Payload
			reqIndex = entry.Index
			reqSchedulerID = entry.SchedulerID
		}
	}

	req := &pb.AppendRequest{
		OriginNodeId: leaderID,
		Term:         currentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Index:        reqIndex,
		SchedulerId:  reqSchedulerID,
		Payload:      payload,
	}

	return req, len(payload) > 0
}

// processHeartbeatResponse evaluates the peer's consensus status and updates tracking indices.
func (n *Node) processHeartbeatResponse(peer core.PeerState, resp *pb.AppendResponse, ni int64, hasPayload bool, nextIndex *sync.Map) {
	switch resp.Status {
	case pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS:
		if hasPayload {
			nextIndex.Store(peer.ID(), ni+1)
		}

	case pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD, pb.AppendResponseStatus_APPEND_RESPONSE_NEEDS_SNAPSHOT:
		if n.checkStepDown(resp.PrevLogTerm, peer.ID()) {
			return
		}

		if ni > 1 {
			nextIndex.Store(peer.ID(), ni-1)
		}
	}
}
