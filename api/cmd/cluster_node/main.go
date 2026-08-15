package main

import (
	"NotAHiveMind/internal/cluster/config"
	"NotAHiveMind/internal/cluster/container"
	"NotAHiveMind/internal/cluster/core"
	"NotAHiveMind/internal/cluster/scheduler"
	"NotAHiveMind/internal/cluster/service"
	"NotAHiveMind/internal/filehandler"
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

//go:generate protoc -I=../../ --go_out=../../ --go_opt=module=NotAHiveMind --go-grpc_out=../../ --go-grpc_opt=module=NotAHiveMind ../../api/proto/cluster/v1/cluster.proto

var (
	env *config.Machine

	ctx    context.Context
	cancel context.CancelFunc
	cli    *client.Client

	nodeServer *service.ClusterServer
	logger     filehandler.LogWriter

	containers core.ContainerPool
	jobQueue   core.JobQueue
)

func init() {
	var err error
	// Node Env
	configFile := flag.String("config", "config.json", "Path to the cluster configuration file")
	limitsFile := flag.String("limits", "limits.json", "Path to the limits file")
	imagesDir := flag.String("images", "images/", "Path to the images directory")
	flag.Parse()
	env, err = config.ImportEnvironment(*configFile, *limitsFile, *imagesDir)
	if err != nil {
		panic(err)
	}

	// Logging
	file, err := os.OpenFile(env.LogDirectory+"/application.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(file)

	// TODO change back to false after testing
	if err := flag.Set("logtostderr", "true"); err != nil {
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

	logger, err = filehandler.NewLogWriter(env.LogDirectory+"/containers", 0660, 100)
	if err != nil {
		glog.Fatalf("Failed to initialize logger: %v", err)
	}

	containers = container.CreateContainerHandler(cli, &logger, env.JobPayloadDir, func(err error) { glog.Errorf("Container error: %v", err) })
	jobQueue = scheduler.CreateJobsQueue()
}

func main() {
	// Set up context
	defer cancel()

	defer func() {
		glog.V(3).Infof("Shutting down jobQueue")
		jobQueue.Close()
		glog.V(3).Infof("Shutting down gRPC server")
		nodeServer.Stop()
		glog.V(3).Infof("Shutting down Docker containers")
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := containers.Close(cleanupCtx); err != nil {
			glog.Errorf("Error shutting down containers during server shutdown: %v", err)
		}
		glog.V(3).Infof("Shutting down logger")
		logger.Close()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	statusCh := make(chan *core.Job, 100)
	go jobQueue.StreamCompleted(statusCh)
	go logger.AsyncWrite()

	nodeServer = service.NewClusterServer(
		env.MachineName,
		":"+env.SchedulerPort,
		env.Limits.MaxNanoCPULimit,
		env.Limits.MaxMemoryBytesLimit,
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
	go nodeServer.MonitorConnection(ctx)
	defer nodeServer.Stop()

	go func() {
		if err := packJobs(); err != nil {
			glog.Errorf("Packer failed: %v", err)
		}
	}()

	go func() {
		if err := healthChecks(ctx); err != nil {
			glog.Errorf("Health checks failed: %v", err)
		}
	}()

	glog.V(1).Infof("Cluster Node [%s] Listening on [%s]", env.MachineName, env.SchedulerPort)
	<-ctx.Done()
	glog.V(1).Info("Shutdown signal received, gracefully stopping...")
}

func packJobs() error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			glog.Warningf("Packer health check shutting down.")
			return ctx.Err()

		case <-ticker.C:
			go func() {
				err := scheduler.PackNextJob(ctx, env, containers, jobQueue)
				if err != nil {
					glog.Errorf("Error during packing: %v", err)
				}
			}()
		}
	}
}

func healthChecks(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	healthRules := []scheduler.HealthRule{
		scheduler.HealthRuleUnresponsive(),
		scheduler.HealthRuleTimedOut(),
		scheduler.HealthRuleExceededJobLimits(),
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := scheduler.RunHealthRule(ctx, containers, jobQueue, healthRules...); err != nil {
				glog.Errorf("Persistence health check stopped: %v", err)
			}
		}
	}
}
