package main

import (
	"ClusterManager/internal/scheduler/config"
	"ClusterManager/internal/scheduler/packer"
	"ClusterManager/internal/scheduler/service"
	"ClusterManager/internal/scheduler/states"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
)

//go:generate protoc -I=../../ --go_out=../../ --go_opt=module=ClusterManager --go-grpc_out=../../ --go-grpc_opt=module=ClusterManager ../../api/proto/scheduling/v1/coordinate.proto

var (
	state         *states.State
	binPacker     *packer.Packer
	rpcController *service.SchedulerControl

	ctx        context.Context
	cancel     context.CancelFunc
	nodeConfig *config.SchedulerConfig
)

func init() {
	configFile := flag.String("test_config", "test_config.json", "Path to the scheduler-example configuration file")
	flag.Parse()

	var err error
	nodeConfig, err = config.ImportEnvironment(*configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to load test_config: %v", err))
	}

	if err := os.MkdirAll(nodeConfig.LogDirectory, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create log directory: %v", err))
	}
	if err := flag.Set("logtostderr", "true"); err != nil { //TODO: change to false after testing
		panic(err)
	}
	if err := flag.Set("log_dir", nodeConfig.LogDirectory); err != nil {
		panic(err)
	}
	if err := flag.Set("v", "2"); err != nil { // Set verbosity level
		panic(err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	state = states.NewState(nodeConfig.SchedulerID, nodeConfig.SchedulerPort)

	peerMap := make(map[string]string)
	for _, peer := range nodeConfig.PeerSchedulers {
		peerMap[peer.PeerID] = peer.PeerPort
		state.AddPeerScheduler(peer.PeerID)
	}
	clusterMap := make(map[string]string)
	for _, cluster := range nodeConfig.Clusters {
		clusterMap[cluster.PeerID] = cluster.PeerPort
		state.AddLocalCluster(cluster.PeerID)
	}
	rpcController, err = service.NewSchedulerControl(ctx, state, nodeConfig.RaftPorts, peerMap, clusterMap)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize RPC Controller: %v", err))
	}

	binPacker = packer.NewPacker(rpcController, state)

	glog.Info("Finished initializing scheduler-example control")
}

func main() {
	defer cancel()
	defer glog.Flush()

	// Capture OS shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Start rpcController
	go func() {
		if err := rpcController.Start(); err != nil {
			glog.Fatalf("Scheduler RPC Control Plane failed: %v", err)
		}
	}()
	defer rpcController.Stop()

	go binPacker.Start(ctx)

	glog.V(1).Infof("Scheduler Node [%s] Listening on [%s]", nodeConfig.SchedulerID, nodeConfig.SchedulerPort)
	<-ctx.Done()
	glog.V(1).Info("Shutdown signal received, gracefully stopping...")
}
