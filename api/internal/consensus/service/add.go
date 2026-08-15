package service

import (
	pb "NotAHiveMind/api/gen/consensus/v1"
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/consensus/filesystem"
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
		if n.state.Role() != core.Leader {
			n.mutex.Unlock()
			n.reqRegistry.Resolve(req.RequestId, false)
			continue
		}

		currentTerm := n.state.CurrentTerm()
		nextIdx := n.FileHandler.Index() + 1
		prevLogIndex := nextIdx - 1
		prevLogTerm := n.getPrevLogTerm(prevLogIndex)

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

		totalNodes := len(peers) + 1
		majorityNeeded := (totalNodes / 2) + 1
		var successfulAcks int64 = 1

		var wg sync.WaitGroup
		var once sync.Once
		quorumReached := make(chan struct{}, 1)

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

				switch resp.Status {
				case pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS:
					n.handleAppendSuccess(&successfulAcks, majorityNeeded, &once, quorumReached)
				case pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD, pb.AppendResponseStatus_APPEND_RESPONSE_NEEDS_SNAPSHOT:
					n.handleAppendRejection(ctx, p, nextIdx, resp)
				}
			}(peer)
		}

		go func() {
			wg.Wait()
			once.Do(func() { close(quorumReached) })
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
func (n *Node) resolvePeerLag(ctx context.Context, peer core.PeerState, failedIndex int64, initialStatus pb.AppendResponseStatus) {
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
		currentTerm := n.state.CurrentTerm()
		leaderID := n.state.ID()
		leaderLastIndex := n.FileHandler.Index()
		discIndex := n.FileHandler.DiscIndex()
		discTerm := n.FileHandler.DiscTerm()
		n.mutex.Unlock()

		if targetIndex > leaderLastIndex {
			glog.V(1).Infof("Peer [%s] is fully caught up to leader index %d", peerID, leaderLastIndex)
			return
		}

		if status == pb.AppendResponseStatus_APPEND_RESPONSE_NEEDS_SNAPSHOT || targetIndex <= discIndex {
			glog.V(2).Infof("Peer [%s] index %d is behind snapshot boundary %d. Initiating stream...", peerID, targetIndex, discIndex)
			if err := n.sendSnapshotToPeer(ctx, peer, discIndex, discTerm); err != nil {
				glog.Errorf("Failed to stream snapshot to peer [%s]: %v", peerID, err)
				return
			}
			targetIndex = discIndex + 1
			status = pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS
			continue
		}

		// Log Rollback Construction
		prevLogIndex := targetIndex - 1
		prevLogTerm := n.getPrevLogTerm(prevLogIndex) // Extracted to helper

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

		switch resp.Status {
		case pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS:
			glog.V(3).Infof("Peer %s accepted log index %d", peerID, targetIndex)
			targetIndex++
			status = pb.AppendResponseStatus_APPEND_RESPONSE_SUCCESS

		case pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD, pb.AppendResponseStatus_APPEND_RESPONSE_NEEDS_SNAPSHOT:
			if n.checkStepDown(resp.PrevLogTerm, peerID) {
				return
			}

			status = resp.Status
			if status == pb.AppendResponseStatus_APPEND_RESPONSE_BAD_PAYLOAD {
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

			if n.checkStepDown(resp.Term, peer.ID()) {
				return fmt.Errorf("leader stepped down during snapshot transmission due to higher term %d on peer %s", resp.Term, peer.ID())
			}

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

// getPrevLogTerm determines the term of the previous log entry, handling snapshot boundaries.
func (n *Node) getPrevLogTerm(prevLogIndex int64) int64 {
	if prevLogIndex == n.FileHandler.DiscIndex() {
		return n.FileHandler.DiscTerm()
	} else if prevLogIndex > 0 {
		entries := n.FileHandler.Entries(prevLogIndex, prevLogIndex+1)
		if len(entries) > 0 {
			return entries[0].Term
		}
	}
	return 0
}

// handleAppendSuccess atomically registers a successful peer replication and closes the quorum channel if majority is reached.
func (n *Node) handleAppendSuccess(acks *int64, majority int, once *sync.Once, ch chan struct{}) {
	currentAcks := atomic.AddInt64(acks, 1)
	if int(currentAcks) >= majority {
		once.Do(func() { close(ch) })
	}
}

// handleAppendRejection processes a peer rejection. It checks if the leader needs to step down, or triggers a catchup routine.
func (n *Node) handleAppendRejection(ctx context.Context, peer core.PeerState, nextIdx int64, resp *pb.AppendResponse) {
	if n.checkStepDown(resp.PrevLogTerm, peer.ID()) {
		return
	}
	// Background worker to resolve peer lag
	go n.resolvePeerLag(ctx, peer, nextIdx, resp.Status)
}

// checkStepDown safely evaluates if the current node needs to revert to a follower based on a newer term from a peer.
func (n *Node) checkStepDown(peerTerm int64, peerID string) bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if peerTerm > n.state.CurrentTerm() {
		glog.V(1).Infof("Stepping down. Found higher term %d on peer [%s]", peerTerm, peerID)
		n.state.BecomeFollower()
		n.state.SetCurrentTerm(peerTerm)
		return true
	}
	return false
}
