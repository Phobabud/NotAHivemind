package service

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"ClusterManager/internal/consensus/core"
	"ClusterManager/internal/consensus/filesystem"
	"ClusterManager/internal/consensus/state"
	"context"
	"errors"
	"net"
	"sync"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedConsensusCoordinationServiceServer

	state       core.SelfState
	grpcServer  *grpc.Server
	peerNodes   map[string]core.PeerState
	FileHandler *filesystem.Handler

	appendChan  chan *pb.RawAppendRequest
	reqRegistry *requestRegistry

	// Volatile state machine tracking fields
	commitIndex int64

	mutex sync.Mutex
}

func NewNode(ctx context.Context, selfID string, selfAddress string, filehandler *filesystem.Handler, peers []core.PeerState) *Node {
	var s core.SelfState = state.New(selfID, selfAddress)

	n := &Node{
		state:       s,
		peerNodes:   make(map[string]core.PeerState),
		appendChan:  make(chan *pb.RawAppendRequest, 100),
		FileHandler: filehandler,
		reqRegistry: newRequestRegistry(),
		commitIndex: filehandler.DiscIndex(),
	}

	go n.startServer()
	go n.ConnectToPeers(ctx, peers)
	go n.AddLog(ctx)

	return n
}

func (n *Node) startServer() {
	lis, err := net.Listen("tcp", n.state.Address())
	if err != nil {
		glog.Fatalf("failed to listen: %v", err)
	}
	defer func(lis net.Listener) {
		err := lis.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			glog.Errorf("failed to close listener: %v", err)
		}
	}(lis)

	grpcServer := grpc.NewServer()
	n.grpcServer = grpcServer
	pb.RegisterConsensusCoordinationServiceServer(grpcServer, n)
	glog.V(2).Info("Serving on " + n.state.Address())
	if err := grpcServer.Serve(lis); err != nil {
		glog.Fatalf("failed to serve: %v", err)
	}
}

// Peers returns a thread-safe snapshot slice of the current active peer list.
func (n *Node) Peers() []core.PeerState {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	peers := make([]core.PeerState, 0, len(n.peerNodes))
	for _, peer := range n.peerNodes {
		peers = append(peers, peer)
	}
	return peers
}

// ConnectToPeers registers all remote endpoints and spins up their respective connection watchdogs.
func (n *Node) ConnectToPeers(ctx context.Context, peers []core.PeerState) {
	for _, peer := range peers {
		if peer.ID() == n.state.ID() {
			continue
		}

		n.mutex.Lock()
		_, exists := n.peerNodes[peer.ID()]
		n.mutex.Unlock()

		if exists {
			continue
		}

		conn, err := grpc.NewClient(peer.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			glog.Errorf("Failed to initialize client connection to peer %s: %v", peer.ID(), err)
			continue
		}

		peer.Disconnected()
		peer.UpdateConnection(conn, pb.NewConsensusCoordinationServiceClient(conn))

		n.mutex.Lock()
		if n.peerNodes != nil {
			n.peerNodes[peer.ID()] = peer
			glog.V(2).Infof("Registered self-healing peer watchdog for %s at %s", peer.ID(), peer.Address())
		} else {
			_ = conn.Close()
			n.mutex.Unlock()
			continue
		}
		n.mutex.Unlock()

		go n.monitorPeerConnection(ctx, peer)
	}
}

func (n *Node) Close() {
	n.mutex.Lock()
	server := n.grpcServer
	peers := n.peerNodes
	n.grpcServer = nil
	n.peerNodes = nil
	close(n.appendChan)
	n.mutex.Unlock()

	if server != nil {
		server.Stop()
	}

	for _, peer := range peers {
		if peer != nil {
			if peer.Conn() != nil {
				_ = peer.Conn().Close()
			}
			peer.Disconnected()
		}
	}
}
