package main

import (
	"ClusterManager/internal/cluster/config"
	"ClusterManager/internal/cluster/container"
	"ClusterManager/internal/cluster/core"
	"ClusterManager/internal/cluster/scheduler"
	"ClusterManager/internal/cluster/service"
	"ClusterManager/internal/filehandler"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/docker/docker/client"
	"github.com/golang/glog"
)

//go:generate protoc -I=../../ --go_out=../../ --go_opt=module=ClusterManager --go-grpc_out=../../ --go-grpc_opt=module=ClusterManager ../../api/proto/cluster/v1/cluster.proto

var (
	env *config.Machine

	ctx    context.Context
	cancel context.CancelFunc
	cli    *client.Client

	logger filehandler.LogWriter

	containers core.ContainerPool
	jobQueue   core.JobQueue
)

func init() {
	var err error
	// Node Env
	env, err = config.ImportEnvironment("", "", "")
	if err != nil {
		panic(err)
	}

	// Logging
	file, err := os.OpenFile("logs/application.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(file)

	if err := flag.Set("logtostderr", "false"); err != nil {
		log.Fatalf("Failed to set logtostderr flag: %v", err)
	}
	if err := flag.Set("log_dir", env.LogDirectory); err != nil {
		log.Fatalf("Failed to set log_dir flag: %v", err)
	}
	if err := flag.Set("v", strconv.Itoa(env.LogVerbosity)); err != nil {
		log.Fatalf("Failed to set v flag: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	cli, err = client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatalf("%v", err)
	}

	logger = filehandler.NewLogWriter("logs/containers/", 0660, 100)

	containers = container.CreateContainerHandler(cli, &logger, nil)
	jobQueue = scheduler.CreateJobsQueue()
}

func main() {
	// Set up context
	defer cancel()
	defer jobQueue.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		glog.Warningf("Received shutdown signal. Initiating graceful shutdown...")
		cancel()
	}()

	statusCh := make(chan *core.Job, 100)
	go jobQueue.StreamCompleted(statusCh)

	nodeServer := service.NewClusterServer(
		env.MachineName,
		":"+env.SchedulerPort,
		env.Limits.MaxCPULimit,
		env.Limits.MaxMemoryLimit,
		jobQueue,
		containers,
		env.Images,
		statusCh,
	)

	go func() {
		if err := nodeServer.Start(); err != nil {
			glog.Fatalf("Cluster Server failed: %v", err)
		}
	}()
	defer nodeServer.Stop()

	go func() {
		if err := packJobs(); err != nil {
			glog.Errorf("Packer failed: %v", err)
		}
	}()

	<-ctx.Done()
	glog.Info("Shutdown complete.")
}

func packJobs() error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	healthRules := []scheduler.HealthRule{
		scheduler.HealthRuleUnresponsive(),
		scheduler.HealthRuleTimedOut(),
		scheduler.HealthRuleExceededJobLimits(),
	}

	for {
		select {
		case <-ctx.Done():
			glog.Warningf("Packer health check shutting down.")
			return ctx.Err()

		case <-ticker.C:
			if err := scheduler.RunHealthRule(ctx, containers, jobQueue, healthRules...); err != nil {
				glog.Errorf("Persistence health check stopped: %v", err)
			}

			err := scheduler.PackNextJob(ctx, env, containers, jobQueue)
			if err != nil {
				glog.Errorf("Error during packing: %v", err)
			}
		}
	}
}
