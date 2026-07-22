package service

import (
	"ClusterManager/internal/consensus/core"
	"context"

	"github.com/golang/glog"
	"google.golang.org/grpc/connectivity"
)

func (n *Node) NumberConnected() int {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	count := 0
	for _, peer := range n.peerNodes {
		if peer.Connection() {
			count++
		}
	}
	return count
}

// monitorPeerConnection watches connection changes dynamically for a single peer.
// Spawns exactly once per client connection, preventing unbounded OOM memory leaks.
func (n *Node) monitorPeerConnection(ctx context.Context, peer core.PeerState) {
	conn := peer.Conn()
	if conn == nil {
		return
	}

	// Prompt the gRPC client to immediately start connection attempts
	conn.Connect()

	for {
		currentState := conn.GetState()
		// Blocks until the gRPC channel state transitions or context is closed
		if !conn.WaitForStateChange(ctx, currentState) {
			return // Context canceled, shutdown complete
		}

		newState := conn.GetState()
		if newState == connectivity.Ready {
			// Only flag as connected if the state transitions from inactive
			if !peer.Connection() {
				glog.V(2).Infof("Connection to peer %s restored/active: %s", peer.ID(), newState)
				peer.Connected()
			}
		} else if newState == connectivity.Idle || newState == connectivity.TransientFailure {
			conn.Connect() // Attempt reconnect

			if peer.Connection() {
				glog.Warningf("Connection to peer %s dropped/inactive: %s", peer.ID(), newState)
				peer.Disconnected()
			}
		}
	}
}

func (n *Node) RecordContact() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.state.RecordContact()
}

func (n *Node) IsElectionTimeout() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return n.state.IsElectionTimeout()
}
