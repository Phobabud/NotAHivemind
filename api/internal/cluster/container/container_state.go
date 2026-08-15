package container

import (
	"NotAHiveMind/internal/cluster/core"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (c *Container) Id() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.id
}

func (c *Container) Image() *core.Image {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.image
}

func (c *Container) JobID() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.assignedJobID
}

func (c *Container) State() core.ContainerState {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.state
}

func (c *Container) Reserve() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.state != core.Free {
		return false
	}
	c.state = core.Reserved
	return true
}

func (c *Container) Release() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.state = core.Free
}

func (c *Container) AssignJob(jobId string, payload json.RawMessage) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.dockerID == "" {
		return fmt.Errorf("container %s has not been created, cannot assign job", c.id)
	}
	if c.state == core.Running {
		return fmt.Errorf("container %s has already been occupied", c.id)
	}

	payloadDir := filepath.Join(c.payloadLocation, c.id, "payload")
	if err := os.MkdirAll(payloadDir, 0755); err != nil {
		return fmt.Errorf("failed to create payload directory: %w", err)
	}

	// Clean up previous jobs
	entries, err := os.ReadDir(payloadDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read payload directory: %w", err)
	}

	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(payloadDir, entry.Name())); err != nil {
			return fmt.Errorf("failed to cleanup old payload file %s: %w", entry.Name(), err)
		}
	}

	// Write file
	tmpPath := filepath.Join(payloadDir, fmt.Sprintf("job-%s.tmp", jobId))
	finalPath := filepath.Join(payloadDir, fmt.Sprintf("job-%s.json", jobId))
	if err := os.WriteFile(tmpPath, payload, 0644); err != nil {
		return fmt.Errorf("failed to write job payload: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to finalize job payload: %w", err)
	}

	c.state = core.Running
	c.assignedJobID = jobId
	c.totalJobs++
	c.lastStateChange = time.Now()

	return nil
}

func (c *Container) WaitForJobResult(ctx context.Context) json.RawMessage {
	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-c.JobsResultsChan:
			return result
		}
	}
}

func (c *Container) FreeJob() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.dockerID == "" {
		return fmt.Errorf("container %s has not been created, cannot free job", c.id)
	}
	c.state = core.Free
	c.assignedJobID = ""
	c.lastStateChange = time.Now()
	return nil
}

func (c *Container) TotalJobs() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.totalJobs
}

func (c *Container) LastStateChange() time.Time {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.lastStateChange
}

func (c *Container) LocalPort() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.localPort
}

func (c *Container) Persistent() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.image.Persistent
}
