package filesystem

import (
	"NotAHiveMind/internal/models"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// makeMockJobPayload is a test helper for building dummy json payloads.
func makeMockJobPayload(id string, state models.Status) []byte {
	b, _ := json.Marshal(map[string]interface{}{"id": id, "state": state})
	return b
}

func TestStoreSnapshot(t *testing.T) {
	// Tests for the background goroutine successfully waking up, compressing the log, writing the active.log snapshot, and cleanly rotating the active WAL file.
	dataDir := t.TempDir()

	// Create an initial active log file to simulate an existing environment
	initialLogPath := filepath.Join(dataDir, "1-0.log")
	initialFile, err := os.OpenFile(initialLogPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create initial log file: %v", err)
	}

	// Create an empty snapshot file reference
	snapPath := filepath.Join(dataDir, "active.log")
	snapFile, err := os.OpenFile(snapPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create initial snap file: %v", err)
	}

	handler := &Handler{
		dataDir:          dataDir,
		activeFile:       initialFile,
		snapshotFile:     snapFile,
		entries:          make([]*LogEntry, 0),
		SnapshotRequired: make(chan struct{}, 1),
	}

	// Populate dummy data with states that can be compressed
	handler.entries = append(handler.entries, &LogEntry{Index: 1, Term: 1, Payload: makeMockJobPayload("job-1", models.Pending)})
	handler.entries = append(handler.entries, &LogEntry{Index: 2, Term: 1, Payload: makeMockJobPayload("job-1", models.Running)})
	handler.entries = append(handler.entries, &LogEntry{Index: 3, Term: 1, Payload: makeMockJobPayload("job-2", models.Pending)})
	handler.entries = append(handler.entries, &LogEntry{Index: 4, Term: 1, Payload: makeMockJobPayload("job-2", models.Completed)})

	// Start the background loop
	var wg sync.WaitGroup
	wg.Add(1)

	var storeErr error
	go func() {
		defer wg.Done()
		// StoreSnapshot will block forever listening to the channel unless we close it
		storeErr = handler.StoreSnapshot()
	}()

	t.Cleanup(func() {
		handler.Close() // Safely close files AND the channel
		wg.Wait()       // Prevent Windows lock errors by waiting for the goroutine to exit
	})

	// Trigger the snapshot
	handler.SnapshotRequired <- struct{}{}

	// Give the background routine a tiny moment to process the channel and run the logic
	time.Sleep(100 * time.Millisecond)

	if storeErr != nil {
		t.Fatalf("StoreSnapshot returned an error: %v", storeErr)
	}

	// Test Assertions

	// Verify the in-memory array was truncated.
	// job-1 should survive (Index 2). job-2 completed, so it was purged.
	// Since Index 4 was the cutoff, the active array should be totally empty now because
	// no new in-flight logs arrived during the snapshot.
	handler.mu.RLock()
	activeEntryCount := len(handler.entries)
	handler.mu.RUnlock()

	if activeEntryCount != 0 {
		t.Errorf("Expected active entries to be 0 after rotation, got %d", activeEntryCount)
	}

	// Verify the physical snapshot file was actually written to
	stat, err := os.Stat(snapPath)
	if err != nil {
		t.Fatalf("Snapshot file disappeared: %v", err)
	}
	if stat.Size() == 0 {
		t.Errorf("Snapshot file is completely empty, expected compressed data")
	}

	// Verify file rotation (The old 1-0.log should be closed, and a new 1-4.log created)
	expectedNewLog := filepath.Join(dataDir, "1-4.log")
	if _, err := os.Stat(expectedNewLog); os.IsNotExist(err) {
		t.Errorf("Expected new active log file %s to be created, but it was not found", expectedNewLog)
	}
}

func TestSnapshotReader(t *testing.T) {
	// Thread-safe generation of a read-only snapshot handle
	dataDir := t.TempDir()
	snapPath := filepath.Join(dataDir, "active.log")

	// Write some dummy data to the physical file
	dummyData := []byte("mock-snapshot-data")
	if err := os.WriteFile(snapPath, dummyData, 0644); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	snapFile, err := os.OpenFile(snapPath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	handler := &Handler{
		snapshotFile: snapFile,
	}

	t.Cleanup(func() {
		handler.Close()
	})

	reader, size, err := handler.SnapshotReader()
	if err != nil {
		t.Fatalf("SnapshotReader failed: %v", err)
	}
	defer reader.Close()

	if size != int64(len(dummyData)) {
		t.Errorf("Expected size %d, got %d", len(dummyData), size)
	}

	// Read from the returned handle to ensure it works
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		t.Fatalf("Failed to read from SnapshotReader handle: %v", err)
	}

	if buf.String() != string(dummyData) {
		t.Errorf("Expected %s, got %s", dummyData, buf.String())
	}
}

func TestInstallSnapshot(t *testing.T) {
	// Safely handling a multi-part stream of snapshot chunks from a Raft Leader, avoiding file corruption by writing to .tmp first, and finally rotating it to active.log.

	dataDir := t.TempDir()

	handler := &Handler{
		dataDir: dataDir,
	}

	t.Cleanup(func() {
		handler.Close()
	})

	// Chunk 1: The beginning of the file
	err := handler.InstallSnapshot(0, []byte("chunk-1-"), false, 0, 0)
	if err != nil {
		t.Fatalf("InstallSnapshot failed on first chunk: %v", err)
	}

	// Ensure active.log wasn't touched yet
	if _, err := os.Stat(filepath.Join(dataDir, "active.log")); !os.IsNotExist(err) {
		t.Errorf("active.log should not be modified until the snapshot stream is complete")
	}

	// Chunk 2: The end of the file (Done = true)
	err = handler.InstallSnapshot(8, []byte("chunk-2"), true, 5, 2)
	if err != nil {
		t.Fatalf("InstallSnapshot failed on final chunk: %v", err)
	}

	// Verify the final rotation
	finalPath := filepath.Join(dataDir, "active.log")
	finalData, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("Failed to read final snapshot: %v", err)
	}

	expectedString := "chunk-1-chunk-2"
	if string(finalData) != expectedString {
		t.Errorf("Expected snapshot contents %q, got %q", expectedString, string(finalData))
	}

	// Verify Raft Indices were updated properly
	if handler.discIndex != 5 || handler.discTerm != 2 {
		t.Errorf("Expected discIndex 5 and discTerm 2, got %d and %d", handler.discIndex, handler.discTerm)
	}
}
