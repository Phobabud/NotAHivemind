package main

import (
	"ClusterManager/internal/consensus/config"
	"ClusterManager/internal/consensus/core"
	"ClusterManager/internal/consensus/filesystem"
	"ClusterManager/internal/consensus/service"
	"ClusterManager/internal/consensus/state"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/golang/glog"
)

//go:generate protoc -I=../../ --go_out=../../ --go_opt=module=ClusterManager --go-grpc_out=../../ --go-grpc_opt=module=ClusterManager ../../api/proto/consensus/v1/consensus.proto

var (
	nodeConfig *config.NodeConfig
	ctx        context.Context
	cancel     context.CancelFunc

	node    *service.Node
	storage *filesystem.Handler
	peers   []core.PeerState
)

func init() {
	var err error
	configPath := flag.String("config", "configs/consensus/example.json", "Path to the cluster configuration file")
	flag.Parse()

	nodeConfig, err = config.ImportEnvironment(*configPath)
	if err != nil {
		panic(err)
	}

	// Logging (std logs for init)
	file, err := os.OpenFile(nodeConfig.LogDirectory+"application.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(file)

	if err := flag.Set("logtostderr", nodeConfig.LogOutputToStdErr); err != nil {
		log.Fatalf("Failed to set logtostderr flag: %v", err)
	}
	if err := flag.Set("log_dir", nodeConfig.LogDirectory); err != nil {
		log.Fatalf("Failed to set log_dir flag: %v", err)
	}
	if err := flag.Set("v", strconv.Itoa(nodeConfig.LogVerbosity)); err != nil {
		log.Fatalf("Failed to set v flag: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	glog.V(1).Info("Loading Storage")

	storage, err = filesystem.LoadStorage(nodeConfig.ConsensusLogDirectory)
	if err != nil {
		glog.Fatalf("Failed to load storage: %v", err)
	}

	glog.V(1).Info("Loading Peers")

	peers = make([]core.PeerState, 0)
	for _, peer := range nodeConfig.Peers {
		peers = append(peers, state.New(peer.NodeID, peer.NodeAddress))
	}

	glog.V(1).Info("Starting Node...")
	node = service.NewNode(ctx, nodeConfig.NodeID, nodeConfig.NodePort, storage, peers)
	if err := waitForMajorityConnected(ctx, node, len(peers)); err != nil {
		glog.Fatalf("Failed to wait for majority connected: %v", err)
	}
	node.RecordContact()
}

func main() {
	defer func() {
		cancel()
		node.Close()
		if err := storage.Close(); err != nil {
			glog.Warningf("Failed to close storage: %v", err)
		}
		glog.Warningf("Node shutdown complete.")
	}()
	glog.V(1).Info("Starting consensus cluster")

	// Start asynchronous snapshot storage thread
	go func() {
		if err := storage.StoreSnapshot(); err != nil {
			glog.Fatalf("Failed to store snapshot: %v", err)
		}
	}()

	go runRaftLoops(ctx, node)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	glog.Info("Shutdown signal received, gracefully stopping...")
}

// waitForMajorityConnected blocks until a majority of the cluster nodes are healthy.
func waitForMajorityConnected(ctx context.Context, node *service.Node, numberOfPeers int) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 30*time.Second)
	defer timeoutCancel()

	majorityNeeded := (numberOfPeers + 1) / 2

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timed out waiting for majority connected")
		case <-ticker.C:
			if node.NumberConnected() >= majorityNeeded {
				return nil
			}
		}
	}
}

func runRaftLoops(ctx context.Context, n *service.Node) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.IsElectionTimeout() {
				n.StartElection(ctx)
			}
		}
	}
}
