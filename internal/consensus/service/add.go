package service

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"ClusterManager/internal/consensus/core"
	"ClusterManager/internal/consensus/filesystem"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

// activeCatchup prevents the leader from spawning catch up routines on the same follower
var activeCatchup sync.Map

// AddLog processes append requests from if it's the leader, adding and distributing them across nodes
func (n *Node) AddLog(ctx context.Context) {
	for req := range n.appendChan {
		n.mutex.Lock()
		// Must be a leader to process
		if n.state.Role() != core.Leader {
			n.mutex.Unlock()
			n.reqRegistry.Resolve(req.RequestId, false)
			continue
		}

		currentTerm := n.state.CurrentTerm()
		nextIdx := n.FileHandler.Index() + 1
		prevLogIndex := nextIdx - 1
		var prevLogTerm int64 = 0

		// Verify continuity
		if prevLogIndex == n.FileHandler.DiscIndex() {
			prevLogTerm = n.FileHandler.DiscTerm()
		} else if prevLogIndex > 0 {
			entries := n.FileHandler.Entries(prevLogIndex, prevLogIndex+1)
			if len(entries) > 0 {
				prevLogTerm = entries[0].Term
			}
		}

		logEntry := &filesystem.LogEntry{
			Index:       nextIdx,
			Term:        currentTerm,
			SchedulerID: req.SchedulerId,
			Payload:     req.Payload,
		}

		if err := n.FileHandler.Append(logEntry); err != nil {
			glog.Errorf("Failed to append log entry locally: %v", err)
			n.mutex.Unlock()
			n.reqRegistry.Resolve(req.RequestId, false)
			continue
		}

		leaderID := n.state.ID()

		peers := make([]core.PeerState, 0, len(n.peerNodes))
		for _, peer := range n.peerNodes {
			if peer.Connection() {
				peers = append(peers, peer)
			}
		}
		n.mutex.Unlock()

		appendRequest := &pb.AppendRequest{
			OriginNodeId: leaderID,
			RequestId:    req.RequestId,
			Term:         currentTerm,
			Index:        nextIdx,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			SchedulerId:  req.SchedulerId,
			Payload:      req.Payload,
		}

		// Calculate and prepare to distribute to majority
		totalNodes := len(peers) + 1
		majorityNeeded := (totalNodes / 2) + 1
		var successfulAcks int64 = 1
		var wg sync.WaitGroup

		quorumReached := make(chan struct{}, 1)
		var once sync.Once

		for _, peer := range peers {
			wg.Add(1)
			go func(p core.PeerState) {
				defer wg.Done()

				rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()

				resp, err := p.Client().AppendEntries(rpcCtx, appendRequest)
				if err != nil {
					glog.V(2).Infof("Failed to replicate to peer %s: %v", p.ID(), err)
					return
				}

				if resp.Status == 1 {
					acks := atomic.AddInt64(&successfulAcks, 1)
					if int(acks) >= majorityNeeded { // If we hit majority, we can end early
						once.Do(func() {
							close(quorumReached)
						})
					}
				} else if resp.Status == 2 || resp.Status == 3 {
					n.mutex.Lock()
					if resp.PrevLogTerm > n.state.CurrentTerm() {
						glog.V(1).Infof("Stepping down. Found higher term %d on peer %s", resp.PrevLogTerm, p.ID())
						n.state.BecomeFollower()
						n.state.SetCurrentTerm(resp.PrevLogTerm)
						n.mutex.Unlock()
						return
					}
					n.mutex.Unlock()

					// Background worker to resolve peer lag
					go n.resolvePeerLag(ctx, p, nextIdx, resp.Status)
				}
			}(peer)
		}

		// Wait for results
		go func() {
			wg.Wait()
			once.Do(func() {
				close(quorumReached)
			})
		}()

		select {
		case <-quorumReached:
			if atomic.LoadInt64(&successfulAcks) >= int64(majorityNeeded) {
				glog.V(2).Infof("Successfully replicated log index %d to majority. Resolving request %s", nextIdx, req.RequestId)

				n.mutex.Lock()
				if nextIdx > n.commitIndex {
					n.commitIndex = nextIdx
				}
				n.mutex.Unlock()

				n.reqRegistry.Resolve(req.RequestId, true)
			} else {
				glog.V(2).Infof("Failed to reach quorum for log index %d. Resolving as false.", nextIdx)
				n.reqRegistry.Resolve(req.RequestId, false)
			}
		case <-ctx.Done():
			n.reqRegistry.Resolve(req.RequestId, false)
			return
		}
	}
}

// resolvePeerLag runs asynchronously in a spinoff goroutine to catch up a lagging peer.
func (n *Node) resolvePeerLag(ctx context.Context, peer core.PeerState, failedIndex int64, initialStatus int32) {
	peerID := peer.ID()

	if _, loaded := activeCatchup.LoadOrStore(peerID, true); loaded {
		return
	}
	defer activeCatchup.Delete(peerID)

	glog.V(1).Infof("Starting catchup loop for peer [%s] (index: %d, initial status: %d)", peerID, failedIndex, initialStatus)

	targetIndex := failedIndex - 1
	status := initialStatus

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n.mutex.Lock()
		if n.state.Role() != core.Leader {
			n.mutex.Unlock()
			return
		}
		currentTerm := n.state.CurrentTerm() // Logical term
		leaderID := n.state.ID()
		leaderLastIndex := n.FileHandler.Index()
		discIndex := n.FileHandler.DiscIndex()
		discTerm := n.FileHandler.DiscTerm()
		n.mutex.Unlock()

		if targetIndex > leaderLastIndex {
			glog.V(1).Infof("Peer [%s] is fully caught up to leader index %d", peerID, leaderLastIndex)
			return
		}

		if status == 3 || targetIndex <= discIndex {
			glog.V(2).Infof("Peer [%s] index %d is behind snapshot boundary %d. Initiating snapshot stream...", peerID, targetIndex, discIndex)

			err := n.sendSnapshotToPeer(ctx, peer, discIndex, discTerm)
			if err != nil {
				glog.Errorf("Failed to stream snapshot to peer [%s]: %v", peerID, err)
				return
			}

			targetIndex = discIndex + 1
			status = 1
			continue
		}

		// Log Rollback
		prevLogIndex := targetIndex - 1
		var prevLogTerm int64 = 0

		if prevLogIndex == discIndex {
			prevLogTerm = discTerm
		} else if prevLogIndex > 0 {
			entries := n.FileHandler.Entries(prevLogIndex, prevLogIndex+1)
			if len(entries) > 0 {
				prevLogTerm = entries[0].Term
			}
		}

		endIndex := targetIndex + 1
		if endIndex > leaderLastIndex+1 {
			endIndex = leaderLastIndex + 1
		}

		entriesToReplicate := n.FileHandler.Entries(prevLogIndex, endIndex)
		var payload []byte = nil
		var reqIndex int64 = 0
		var reqSchedulerID = ""

		for _, entry := range entriesToReplicate {
			if entry.Index == prevLogIndex {
				prevLogTerm = entry.Term
			}
			if entry.Index == targetIndex {
				payload = entry.Payload
				reqIndex = entry.Index
				reqSchedulerID = entry.SchedulerID
			}
		}

		appendRequest := &pb.AppendRequest{
			OriginNodeId: leaderID,
			Term:         currentTerm,
			Index:        reqIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			SchedulerId:  reqSchedulerID,
			Payload:      payload,
		}

		rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := peer.Client().AppendEntries(rpcCtx, appendRequest)
		cancel()

		if err != nil {
			glog.V(2).Infof("Communication failure with peer %s: %v", peerID, err)
			return
		}

		if resp.Status == 1 {
			glog.V(3).Infof("Peer %s accepted log index %d", peerID, targetIndex)
			targetIndex++
			status = 1
		} else if resp.Status == 2 || resp.Status == 3 {
			n.mutex.Lock()
			if resp.PrevLogTerm > n.state.CurrentTerm() { // Logical term
				glog.V(1).Infof("Stepping down. Found higher term %d on peer [%s]", resp.PrevLogTerm, peerID)
				n.state.BecomeFollower()
				n.state.SetCurrentTerm(resp.PrevLogTerm)
				n.mutex.Unlock()
				return
			}
			n.mutex.Unlock()

			status = resp.Status
			if status == 2 {
				targetIndex--
				if targetIndex < 1 {
					glog.Warningf("Cannot roll back below index 1 for peer [%s]", peerID)
					return
				}
			}
		}
	}
}

// sendSnapshotToPeer streams the actual compressed active.log binary contents sequentially across chunked gRPC frames due to gRPC limits
func (n *Node) sendSnapshotToPeer(ctx context.Context, peer core.PeerState, lastIncludedIndex int64, lastIncludedTerm int64) error {
	reader, size, err := n.FileHandler.SnapshotReader()
	if err != nil {
		return fmt.Errorf("failed to open snapshot file for reading: %w", err)
	}
	defer reader.Close()

	const chunkSize = 64 * 1024 // 64 KB Chunks
	buf := make([]byte, chunkSize)
	var offset int64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bytesRead, readErr := reader.Read(buf)
		if bytesRead > 0 {
			done := offset+int64(bytesRead) >= size
			req := &pb.InstallSnapshotRequest{
				LeaderId:          n.state.ID(),
				Term:              n.state.CurrentTerm(), // Logical term
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Offset:            offset,
				Data:              buf[:bytesRead],
				Done:              done,
			}

			rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			resp, err := peer.Client().InstallSnapshot(rpcCtx, req)
			cancel()

			if err != nil {
				return fmt.Errorf("gRPC transmission error at offset %d: %w", offset, err)
			}

			// Stepped down validation
			n.mutex.Lock()
			if resp.Term > n.state.CurrentTerm() { // Logical term
				n.state.BecomeFollower()
				n.state.SetCurrentTerm(resp.Term)
				n.mutex.Unlock()
				return fmt.Errorf("leader stepped down during snapshot transmission due to higher term %d on peer %s", resp.Term, peer.ID())
			}
			n.mutex.Unlock()

			offset += int64(bytesRead)
			if done {
				break
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("snapshot file read failure: %w", readErr)
		}
	}

	glog.V(2).Infof("Successfully transmitted snapshot state (%d bytes) up to index %d to peer [%s]", offset, lastIncludedIndex, peer.ID())
	return nil
}
