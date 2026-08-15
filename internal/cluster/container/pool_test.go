package container

import (
	"NotAHiveMind/internal/cluster/core"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/docker/docker/client"
)

// mockTransport intercepts HTTP requests made by the Docker SDK and returns fake JSON, so we don't need the docker daemon
type mockTransport struct{}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	mockDockerResponse := `{"State": {"Running": true, "Health": {"Status": "healthy"}}}`
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(mockDockerResponse)),
		Header:     make(http.Header),
	}, nil
}

func newMockDockerClient() *client.Client {
	cli, _ := client.NewClientWithOpts(
		client.WithHTTPClient(&http.Client{Transport: &mockTransport{}}),
		client.WithVersion("1.41"), // Hardcoded to prevent API version negotiation ping
	)
	return cli
}

func TestContainers_AddRemoveDeduplication(t *testing.T) {
	pool := &Containers{
		Containers: make([]*Container, 0),
		mutex:      sync.Mutex{},
	}

	c1 := &Container{id: "container-1"}
	c2 := &Container{id: "container-2"}

	_ = pool.Add(c1)
	_ = pool.Add(c2)

	if pool.Len() != 2 {
		t.Fatalf("Expected pool length to be 2, got %d", pool.Len())
	}

	// Test Deduplication
	err := pool.Add(c1)
	if err == nil {
		t.Errorf("Expected error when adding a duplicate container ID")
	}

	// Test Removal
	err = pool.Remove("container-1")
	if err != nil {
		t.Errorf("Failed to remove container: %v", err)
	}

	if pool.Len() != 1 {
		t.Errorf("Expected pool length to be 1 after removal, got %d", pool.Len())
	}
}

func TestContainers_FindFreeContainer(t *testing.T) {
	cli := newMockDockerClient()

	pool := &Containers{
		Containers: make([]*Container, 0),
		mutex:      sync.Mutex{},
	}

	imageType := &core.Image{Alias: "python-worker"}

	cRunning := &Container{id: "c-run", client: cli, dockerID: "hash", image: imageType, state: core.Running, mutex: &sync.RWMutex{}}
	cReserved := &Container{id: "c-res", client: cli, dockerID: "hash", image: imageType, state: core.Reserved, mutex: &sync.RWMutex{}}
	cFree := &Container{id: "c-free", client: cli, dockerID: "hash", image: imageType, state: core.Free, mutex: &sync.RWMutex{}}

	_ = pool.Add(cRunning)
	_ = pool.Add(cReserved)
	_ = pool.Add(cFree)

	ctx := context.Background()

	// Should ignore Running and Reserved, and successfully return cFree
	found, err := pool.FindFreeContainer(ctx, "python-worker")
	if err != nil {
		t.Fatalf("FindFreeContainer failed: %v", err)
	}
	if found.Id() != "c-free" {
		t.Errorf("Expected to find 'c-free', got %s", found.Id())
	}

	// Should throw an error if searching for an image that isn't booted in the cluster
	_, err = pool.FindFreeContainer(ctx, "non-existent-image")
	if !errors.Is(err, core.ErrImageNotFound) {
		t.Errorf("Expected ErrImageNotFound, got %v", err)
	}

	// Should throw an error if all containers for an image are currently busy
	_ = pool.Remove("c-free")
	_, err = pool.FindFreeContainer(ctx, "python-worker")
	if !errors.Is(err, core.ErrAllOccupied) {
		t.Errorf("Expected ErrAllOccupied, got %v", err)
	}
}

func TestContainers_UsedSpace(t *testing.T) {
	pool := &Containers{
		Containers: make([]*Container, 0),
		mutex:      sync.Mutex{},
	}

	// 1 CPU Core = 1,000,000,000 NanoCPUs
	_ = pool.Add(&Container{id: "c1", nanoCPULimit: 2000000000, memBytesLimit: 1024})
	_ = pool.Add(&Container{id: "c2", nanoCPULimit: 4000000000, memBytesLimit: 2048})

	totalCPU, totalMem := pool.UsedSpace()

	if totalCPU != 6000000000 {
		t.Errorf("Expected Total CPU to be 6000000000, got %d", totalCPU)
	}

	if totalMem != 3072 {
		t.Errorf("Expected Total Memory to be 3072, got %d", totalMem)
	}
}
