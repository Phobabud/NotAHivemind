package service

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"ClusterManager/internal/consensus/core"
	"context"
	"sync"
	"time"
)

// BroadcastHeartbeats is used by the Leader to broadcast heartbeats and maintain quorum.
func (n *Node) BroadcastHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var nextIndex sync.Map
	n.mutex.Lock()
	lastLogIdx := n.FileHandler.Index()
	for _, peer := range n.peerNodes {
		nextIndex.Store(peer.ID(), lastLogIdx+1)
	}
	n.mutex.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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

				go func(p core.PeerState) {
					n.mutex.Lock()
					if n.state.Role() != core.Leader || n.state.CurrentTerm() != currentTerm {
						n.mutex.Unlock()
						return
					}
					n.mutex.Unlock()

					val, _ := nextIndex.Load(p.ID())
					ni := val.(int64)

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

					rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					resp, err := p.Client().AppendEntries(rpcCtx, req)
					cancel()

					if err != nil {
						return
					}

					if resp.Status == 1 {
						if len(payload) > 0 {
							nextIndex.Store(p.ID(), ni+1)
						}
						return
					}

					if resp.Status == 2 || resp.Status == 3 {
						n.mutex.Lock()
						if resp.PrevLogTerm > n.state.CurrentTerm() {
							n.state.BecomeFollower()
							n.state.SetCurrentTerm(resp.PrevLogTerm)
							n.mutex.Unlock()
							return
						}
						n.mutex.Unlock()

						// Decouple catch-up sequence from tight CPU spinning loop.
						// The follower's nextIndex decrement is registered here and will cleanly re-transmit on the next scheduled heartbeat.
						if ni > 1 {
							nextIndex.Store(p.ID(), ni-1)
						}
						return
					}
				}(peer)
			}
		}
	}
}
