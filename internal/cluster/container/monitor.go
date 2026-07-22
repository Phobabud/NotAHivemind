package container

import (
	"ClusterManager/internal/filehandler"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
)

func (c *Container) Health(ctx context.Context) (int, error) {
	if c.dockerID == "" {
		return -1, fmt.Errorf("container %s has not been created, cannot check status", c.id)
	}

	inspectData, err := c.client.ContainerInspect(ctx, c.dockerID)
	if err != nil {
		return -1, fmt.Errorf("failed to inspect container: %v", err)
	}

	if !inspectData.State.Running {
		return 3, nil
	}

	if inspectData.State.Health == nil {
		return -1, fmt.Errorf("warning: Container %s has no healthcheck defined. Assuming healthy since it is running", c.dockerID)
	}

	// Possible values are: "starting", "healthy", "unhealthy", or "none"
	switch inspectData.State.Health.Status {
	case "healthy":
		return 0, nil
	case "starting":
		return 1, nil
	case "unhealthy":
		return 2, nil
	case "none":
		return 3, nil
	}

	return -1, fmt.Errorf("unexpected health status for container %s: %s", c.dockerID, inspectData.State.Health.Status)
}

func (c *Container) Usage(ctx context.Context) (float64, uint64, error) {
	c.mutex.Lock()
	// Check to make sure it's been created
	if c.dockerID == "" {
		c.mutex.Unlock()
		return 0.0, 0, fmt.Errorf("container %s has not been created", c.id)
	}

	stats, err := c.client.ContainerStats(ctx, c.dockerID, false)
	c.mutex.Unlock()
	if err != nil {
		return 0.0, 0, fmt.Errorf("failed to get container stats: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil { // Can't do much about this here. It might have already been closed
			log.Printf("Error closing stats body: %v", err)
		}
	}(stats.Body)

	var v container.StatsResponse
	if err = json.NewDecoder(stats.Body).Decode(&v); err != nil {
		return 0.0, 0, fmt.Errorf("failed to decode container stats: %v", err)
	}

	currentMem := v.MemoryStats.Usage

	cpuDelta := float64(v.CPUStats.CPUUsage.TotalUsage) - float64(v.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(v.CPUStats.SystemUsage) - float64(v.PreCPUStats.SystemUsage)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		// Multiply by number of cores to get 0-100% per core style
		return (cpuDelta / systemDelta) * float64(len(v.CPUStats.CPUUsage.PercpuUsage)) * 100.0, currentMem, nil
	}

	return 0.0, currentMem, nil
}

func (c *Container) Running(ctx context.Context) (bool, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check to make sure it's been created
	if c.dockerID == "" {
		return false, fmt.Errorf("container %s has not been created, cannot check status", c.id)
	}

	// See if the container is actively running or not, and return that status
	inspect, err := c.client.ContainerInspect(ctx, c.dockerID)
	if err != nil {
		return false, fmt.Errorf("failed to inspect container: %v", err)
	}

	return inspect.State.Running, nil
}

func (c *Container) monitorForResults(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	resultsPath := filepath.Join(c.payloadLocation, c.id, "results")

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			entries, err := os.ReadDir(resultsPath)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if !entry.Type().IsRegular() {
					continue
				}

				if strings.ToLower(filepath.Ext(entry.Name())) != ".json" {
					continue
				}

				filePath := filepath.Join(resultsPath, entry.Name())

				// Safely process and delete the file while holding the lock
				jsonMsg, err := processAndDeleteFile(filePath)
				if err != nil {
					return err
				}

				c.JobsResultsChan <- jsonMsg
			}
		}
	}
}

func processAndDeleteFile(filePath string) (json.RawMessage, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file contents: %w", err)
	}

	if !json.Valid(fileBytes) {
		return nil, fmt.Errorf("invalid JSON payload structure")
	}
	rawJSONMsg := json.RawMessage(fileBytes)

	file.Close()
	if err := os.Remove(filePath); err != nil {
		return nil, fmt.Errorf("failed to delete file: %w", err)
	}
	fmt.Printf("Safely deleted file: %s\n", filepath.Base(filePath))

	return rawJSONMsg, nil
}

func (c *Container) streamContainerLogs(ctx context.Context) error {
	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	}

	out, err := c.client.ContainerLogs(ctx, c.dockerID, options)
	if err != nil {
		return fmt.Errorf("error opening log stream: %v", err)
	}
	defer out.Close()

	logName := fmt.Sprintf("%s.log", c.id)
	logHeader := []string{
		fmt.Sprintf("-----Log Data for [%s] Image [%s] Started [%s]-----",
			c.id, c.image.Alias, time.Now().Format(time.RFC3339Nano)),
	}

	c.logger.Add(logHeader, logName)
	// Keep the file "Open" in the LogWriter until this function exits

	// Bridge Docker to LogWriter directly
	bridge := &logBridge{
		logger:  c.logger,
		logName: logName,
	}

	// StdCopy handles the 8-byte Docker headers and writes clean bytes to our bridge.
	// We use the bridge for both Stdout and Stderr.
	_, err = stdcopy.StdCopy(bridge, bridge, out)

	if err != nil && err != io.EOF {
		return fmt.Errorf("log stream error for %s: %w", c.id, err)
	}

	return nil
}

// logBridge sends io.Writer logs to filehandler.LogWriter
type logBridge struct {
	logger  *filehandler.LogWriter
	logName string
}

func (w *logBridge) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	// We send the raw byte chunk. The LogWriter handles the actual file I/O.
	// Using string(p) here is safe for Node/Puppeteer output.
	w.logger.Add([]string{string(p)}, w.logName)
	return len(p), nil
}
