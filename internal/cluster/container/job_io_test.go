package container

import (
	"NotAHiveMind/internal/cluster/core"
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestContainer_AtomicJobAssignment(t *testing.T) {
	sandbox := t.TempDir()

	c := &Container{
		id:              "test-io-container",
		dockerID:        "mock-docker-hash",
		payloadLocation: sandbox,
		state:           core.Reserved,
		mutex:           &sync.RWMutex{},
	}

	jobID := "777"
	payload := []byte(`{"command": "compute"}`)

	err := c.AssignJob(jobID, payload)
	if err != nil {
		t.Fatalf("AssignJob failed: %v", err)
	}

	// Verify the directory structure created by AssignJob, should have a payload and results folder
	expectedFilePath := filepath.Join(sandbox, c.id, "payload", "job-777.json")

	data, err := os.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("Expected job file was not found on disk: %v", err)
	}

	if !bytes.Equal(data, payload) {
		t.Errorf("File contents mismatch. Expected %s, got %s", payload, data)
	}

	// Verify that the tmp file was successfully cleaned up/renamed
	tmpFilePath := filepath.Join(sandbox, c.id, "payload", "job-777.tmp")
	if _, err := os.Stat(tmpFilePath); !os.IsNotExist(err) {
		t.Errorf("Expected .tmp file to be deleted or renamed, but it still exists")
	}
}

func TestProcessAndDeleteFile(t *testing.T) {
	// Looks for: Safe parsing of worker results and automatic cleanup
	t.Run("Valid JSON", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "result.json")
		_ = os.WriteFile(filePath, []byte(`{"status": "success"}`), 0644)

		msg, err := processAndDeleteFile(filePath)
		if err != nil {
			t.Fatalf("processAndDeleteFile failed on valid JSON: %v", err)
		}

		if string(msg) != `{"status": "success"}` {
			t.Errorf("Expected JSON message to be extracted intact")
		}

		if _, err := os.Stat(filePath); !os.IsNotExist(err) {
			t.Errorf("Expected file to be physically deleted after processing")
		}
	})

	t.Run("Corrupted JSON", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "corrupted.json")
		_ = os.WriteFile(filePath, []byte(`{invalid-json`), 0644)

		_, err := processAndDeleteFile(filePath)
		if err == nil {
			t.Fatalf("Expected processAndDeleteFile to return an error for invalid JSON")
		}

		// The file should NOT be deleted if it was corrupted, allowing for manual debugging
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Expected corrupted file to be left on disk for debugging")
		}
	})
}

func TestNormalizeHTTPPort(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"8080", "localhost:8080"},
		{"localhost:9090", "localhost:9090"},
		{"127.0.0.1:3000", "127.0.0.1:3000"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeHTTPPort(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeHTTPPort(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}
