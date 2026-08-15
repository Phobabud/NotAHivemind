// Package container contains operations that are related to the management of Containers
package container

import (
	"NotAHiveMind/internal/cluster/core"
	"NotAHiveMind/internal/filehandler"
	"NotAHiveMind/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/golang/glog"
	"golang.org/x/sync/errgroup"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type Container struct {
	// Required Fields
	client        *client.Client
	id            string
	logger        *filehandler.LogWriter
	nanoCPULimit  int64
	memBytesLimit int64
	localPort     string
	image         *core.Image

	// Instance Fields
	dockerID        string // Never use this to identify a container
	payloadLocation string
	args            []string
	mutex           *sync.RWMutex

	// Job Handling
	JobsResultsChan chan json.RawMessage
	totalJobs       int
	state           core.ContainerState
	lastStateChange time.Time
	assignedJobID   string

	// Optional Fields
	volumes []core.Volume
}

// NewContainer initializes a new Container struct based on an Image struct, making setup less complicated for the caller
func NewContainer(cli *client.Client, logger *filehandler.LogWriter, image *core.Image, payloadVolumeLocation string, port string) core.ContainerAPI {
	return &Container{
		client:          cli,
		id:              models.NewContainerID().String(),
		logger:          logger,
		image:           image,
		volumes:         image.Volumes,
		payloadLocation: payloadVolumeLocation,
		JobsResultsChan: make(chan json.RawMessage, 1),
		nanoCPULimit:    image.NanoCPULimit,
		memBytesLimit:   image.MemoryBytesLimit,
		localPort:       normalizeHTTPPort(port),
		totalJobs:       0,
		state:           core.Reserved,
		lastStateChange: time.Now(),
		mutex:           &sync.RWMutex{},
	}
}

func (c *Container) Init(ctx context.Context, args ...string) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.dockerID != "" { //container already exists and can't be created again
		return c.dockerID, fmt.Errorf("container %s already exists with name %s, aborting init", c.id, c.dockerID)
	}
	c.args = args // Save the most recent args

	// Construct volume
	jobsDir := filepath.Join(c.payloadLocation, c.id)
	absJobsDir, err := filepath.Abs(jobsDir)
	glog.V(3).Infof("Creating jobs directory at %s for container %s", absJobsDir, c.id)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for jobs directory: %w", err)
	}
	dockerSafeJobsDir := filepath.ToSlash(absJobsDir)

	if err := os.MkdirAll(absJobsDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create jobs directory: %w", err)
	}
	payloadDir := filepath.Join(absJobsDir, "payload")
	resultDir := filepath.Join(absJobsDir, "results")
	glog.V(3).Infof("Creating payload directory at %s and result directory at %s for container %s", payloadDir, resultDir, c.id)
	if err := os.MkdirAll(payloadDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create payload directory: %w", err)
	}
	if err := os.MkdirAll(resultDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create result directory: %w", err)
	}

	// Construct mounts
	var mounts []mount.Mount
	mounts = append(mounts, mount.Mount{
		Type:   mount.TypeBind,
		Source: dockerSafeJobsDir,
		Target: "/job/",
	})
	for _, volume := range c.volumes {
		bind := mount.TypeBind
		if volume.Type == "volume" {
			bind = mount.TypeVolume
		}
		mounts = append(mounts, mount.Mount{
			Type:   bind,
			Source: volume.Source,
			Target: volume.Target,
		})
	}

	// Create the container
	containerResp, err := c.client.ContainerCreate(ctx, &container.Config{
		Image: c.image.ImageName,
		Cmd:   c.args,
		Env: []string{
			fmt.Sprintf("CONTAINER_NAME=%s", c.id),
			fmt.Sprintf("PORT=%s", c.localPort), // Containers doesn't use this functionally!
			"PYTHONUNBUFFERED=1",
		},
	}, &container.HostConfig{
		Mounts: mounts,
		Resources: container.Resources{
			NanoCPUs: c.nanoCPULimit,
			Memory:   c.memBytesLimit,
		},
		PortBindings: nat.PortMap{
			"3000/tcp": []nat.PortBinding{
				{
					HostIP:   "::",
					HostPort: strings.ReplaceAll(c.localPort, "localhost:", ""),
				},
			},
		},
	}, nil, nil, c.id)
	if err != nil { // Return empty response on failure
		glog.Errorf("Failed to create container: %v", err)
		return "", fmt.Errorf("failed to create container: %v", err)
	}

	// Configure the Name so we don't need the response later, but we'll return it anyway in case
	c.dockerID = containerResp.ID
	c.state = core.Reserved
	c.lastStateChange = time.Now()

	return c.dockerID, nil
}

func (c *Container) Launch(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Check to make sure it's been created
	err := func() error {
		c.mutex.Lock()

		if c.dockerID == "" {
			c.mutex.Unlock()
			return fmt.Errorf("container %s has not been created", c.id)
		}
		glog.V(3).Infof("Launching container %s", c.id)
		if err := c.client.ContainerStart(runCtx, c.dockerID, container.StartOptions{}); err != nil {
			c.mutex.Unlock()
			c.Delete(ctx)
			return fmt.Errorf("failed to start container: %v", err)
		}
		c.mutex.Unlock()
		return nil
	}()
	if err != nil {
		return err
	}

	var g errgroup.Group

	g.Go(func() error {
		return c.streamContainerLogs(runCtx)
	})
	if !c.image.Persistent {
		g.Go(func() error {
			return c.monitorForResults(runCtx, 50*time.Millisecond)
		})
	}

	statusChan, errCh := c.client.ContainerWait(ctx, c.dockerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("container %s finished with error: %v", c.id, err)
		}
	case <-statusChan:
	}

	cancel()

	// Container will finish before the stream finishes, as stream termination is dependent on the container. The unfortunate side effect is an error with the stream during container runtime will remain undetected, and will cause the loss of logs.
	if err := g.Wait(); err != nil {
		return fmt.Errorf("container %s finished with error: %v", c.id, err)
	}

	return nil
}

func (c *Container) Stop(ctx context.Context, timeout int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check to make sure it's been created
	if c.dockerID == "" {
		return fmt.Errorf("container %s has not been created, cannot stop", c.id)
	}

	if timeout < 0 {
		return fmt.Errorf("timeout for stopping container cannot be less than zero")
	}

	stopTime := container.StopOptions{Timeout: &timeout}
	if err := c.client.ContainerStop(ctx, c.dockerID, stopTime); err != nil {
		return fmt.Errorf("failed to stop container: %v", err)
	}

	return nil
}

func (c *Container) Rebuild(ctx context.Context, newPort string) error {
	c.mutex.Lock()
	if c.dockerID == "" {
		c.mutex.Unlock()
		return fmt.Errorf("container %s has not been created, cannot rebuild", c.id)
	}

	// Delete, and rebuild
	c.id = fmt.Sprintf("%s-%s", c.image.Alias, fmt.Sprintf("%d", time.Now().UnixNano()))
	c.localPort = normalizeHTTPPort(newPort)
	c.mutex.Unlock()

	if err := c.Delete(ctx); err != nil {
		return err
	}
	if _, err := c.Init(ctx, c.args...); err != nil {
		return err
	}
	return nil
}

func (c *Container) Cleanup(ctx context.Context) error {
	c.mutex.Lock()
	c.state = core.Reserved
	c.lastStateChange = time.Now()
	c.mutex.Unlock()

	time.Sleep(500 * time.Millisecond)

	return c.Delete(ctx)
}

func (c *Container) Delete(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check to make sure it's been created
	if c.dockerID == "" {
		return fmt.Errorf("container %s has not been created, cannot delete", c.id)
	}

	deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Clean up container, we may not want to reuse the container in the future
	if err := c.client.ContainerRemove(deleteCtx, c.dockerID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("failed to remove container: %v", err)
	}
	c.dockerID = "" // Clear the ID to indicate it's been deleted
	close(c.JobsResultsChan)
	if err := os.RemoveAll(filepath.Join(c.payloadLocation, c.id)); err != nil {
		return fmt.Errorf("failed to remove jobs folder: %v", err)
	}
	c.payloadLocation = ""
	glog.V(2).Infof("Container [%s] and its resources have been deleted", c.id)
	return nil
}

// normalizeHTTPPort is a helper to help normalize a (potentially inconsistent) local HTTP port string for processing.
func normalizeHTTPPort(port string) string {
	if port == "" {
		return ""
	}
	if strings.Contains(port, ":") {
		return port
	}
	return fmt.Sprintf("localhost:%s", port)
}
