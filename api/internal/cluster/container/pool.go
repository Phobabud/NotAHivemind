package container

import (
	"NotAHiveMind/internal/cluster/core"
	"NotAHiveMind/internal/filehandler"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/client"
	"github.com/golang/glog"
)

type Containers struct {
	Containers  []*Container
	mutex       sync.Mutex
	ErrorStream chan error

	client     *client.Client
	logger     *filehandler.LogWriter
	payloadDir string
}

// CreateContainerHandler creates a clean Containers struct given a function which acts like a destination for the error stream
func CreateContainerHandler(client *client.Client, logger *filehandler.LogWriter, payloadDir string, errDestination func(err error)) core.ContainerPool {
	containers := &Containers{
		Containers:  make([]*Container, 0),
		mutex:       sync.Mutex{},
		ErrorStream: make(chan error),
		client:      client,
		logger:      logger,
		payloadDir:  payloadDir,
	}
	go func() { // Automatically shuts down when Close() is called
		for err := range containers.ErrorStream {
			errDestination(err)
		}
	}()
	return containers
}

func (c *Containers) Close(ctx context.Context) error {
	if err := c.DestroyAll(ctx); err != nil {
		return err
	}

	entries, err := os.ReadDir(c.payloadDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		entryPath := filepath.Join(c.payloadDir, entry.Name())
		err := os.RemoveAll(entryPath)
		if err != nil {
			glog.Errorf("failed to remove container directory %s: %s", entryPath, err)
		}
	}

	// Safely close ErrorStream
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.mutex.Lock()
			if c.ErrorStream != nil && len(c.Containers) == 0 {
				close(c.ErrorStream)
				c.ErrorStream = nil
				c.mutex.Unlock()
				return nil
			}
			c.mutex.Unlock()
		}
	}
}

func (c *Containers) DestroyAll(ctx context.Context) error {
	c.mutex.Lock()
	tracked := append([]*Container(nil), c.Containers...)
	c.Containers = make([]*Container, 0)
	c.mutex.Unlock()

	var errs []string
	wg := sync.WaitGroup{}
	for _, container := range tracked {
		if container == nil || container.id == "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := container.Delete(ctx); err != nil {
				errs = append(errs, err.Error())
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("failed to destroy tracked containers: %s", strings.Join(errs, "; "))
	}

	return nil
}

func (c *Containers) Add(targetContainer core.ContainerAPI) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	concreteContainer, ok := targetContainer.(*Container)
	if !ok {
		return fmt.Errorf("failed to add container: invalid type provided")
	}

	for _, container := range c.Containers {
		if container.id == concreteContainer.id {
			return fmt.Errorf("container already exists: %s", concreteContainer.id)
		}
	}
	c.Containers = append(c.Containers, concreteContainer)
	return nil
}

func (c *Containers) AddFromImage(image *core.Image) (core.ContainerAPI, error) {
	port, err := GetFreePort()
	if err != nil {
		return nil, err
	}
	container := NewContainer(c.client, c.logger, image, c.payloadDir, fmt.Sprintf("localhost:%d", port))
	if _, err := container.Init(context.Background(), image.Args...); err != nil {
		return nil, err
	}
	if err := c.Add(container); err != nil {
		return nil, err
	}
	return container, nil
}

func (c *Containers) Remove(ctx context.Context, id string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, container := range c.Containers {
		if container.id == id {
			c.Containers = append(c.Containers[:i], c.Containers[i+1:]...)
			return container.Delete(ctx)
		}
	}
	return fmt.Errorf("container not found: %s", id)
}

func (c *Containers) Get(id string) (core.ContainerAPI, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, container := range c.Containers {
		if container.id == id {
			return container, nil
		}
	}
	return nil, fmt.Errorf("container not found: %s", id)
}

func (c *Containers) FindFreeContainer(ctx context.Context, imageName string) (core.ContainerAPI, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	matchingImage := false
	allOccupied := true
	for _, container := range c.Containers {
		if running, err := container.Running(ctx); err != nil || !running {
			continue
		}

		if container.state == core.Free && container.image.Alias == imageName {
			if !container.Reserve() {
				continue
			}
			return container, nil
		}
		if container.state == core.Free {
			allOccupied = false
		}
		if container.image.Alias == imageName {
			matchingImage = true
		}
	}

	if !matchingImage {
		return nil, core.ErrImageNotFound
	}
	if allOccupied {
		return nil, core.ErrAllOccupied
	}
	return nil, fmt.Errorf("container not found: %s", imageName)
}

func (c *Containers) ActiveImageNames() []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	lst := make([]string, 0)
	for _, container := range c.Containers {
		lst = append(lst, container.image.Alias)
	}
	return lst
}

func (c *Containers) AvailableImageNames(ctx context.Context) []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	lst := make([]string, 0)
	for _, container := range c.Containers {
		// If the container is healthy, and it's not occupied
		if status, err := container.Health(ctx); container.state == core.Free && status == 0 && err == nil {
			lst = append(lst, container.image.Alias)
		}
	}
	return lst
}

func (c *Containers) Len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.Containers)
}

func (c *Containers) ForEach(action func(container core.ContainerAPI) error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, container := range c.Containers {
		if err := action(container); err != nil {
			return err
		}
	}
	return nil
}

func (c *Containers) Rebuild(ctx context.Context) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, container := range c.Containers {
		// Run health checks, which rebuilds containers that have stalled or haven't been launched yet
		status, err := container.Running(ctx)
		healthCode, err := container.Health(ctx)
		if err != nil || !status || !(healthCode == 0 || healthCode == 1) {
			// Rebuild and stream errors back
			go func(target *Container) {
				port, _ := GetFreePort()
				if err := target.Rebuild(ctx, fmt.Sprintf("%d", port)); err != nil {
					c.ErrorStream <- fmt.Errorf("rebuild failed: %w", err)
					return
				}

				if err := target.Launch(ctx); err != nil {
					c.ErrorStream <- fmt.Errorf("launch failed for %s: %w", target.id, err)
				}
			}(container)
		}
	}
}

func (c *Containers) TrackedLaunch(ctx context.Context, id string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var target *Container
	for _, container := range c.Containers {
		if container.Id() == id {
			target = container
			break
		}
	}

	if target == nil {
		c.ErrorStream <- fmt.Errorf("TrackedLaunch aborted: container with id %s not found in pool", id)
		return
	}

	go func() {
		if err := target.Launch(ctx); err != nil {
			c.ErrorStream <- fmt.Errorf("launch failed for %s: %w", target.Id(), err)
		}
	}()
}

func (c *Containers) UsedSpace() (int64, int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	nanoCpuTotal := int64(0)
	memoryTotal := int64(0)
	for _, container := range c.Containers {
		nanoCpuTotal += container.nanoCPULimit
		memoryTotal += container.memBytesLimit
	}
	return nanoCpuTotal, memoryTotal
}

func (c *Containers) Ping() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Ping the Docker daemon
	if _, err := c.client.Ping(ctx); err != nil {
		return false
	}
	return true
}

// GetFreePort requests the system for an unused IPv6 port
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "[::1]:0")
	if err != nil {
		return 0, err
	}

	// Listen on port 0. The OS will assign a random free port.
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	// Grab the port the OS assigned
	port := l.Addr().(*net.TCPAddr).Port

	// Immediately close the listener so Docker can use the port
	err = l.Close()
	if err != nil {
		return 0, err
	}

	return port, nil
}
