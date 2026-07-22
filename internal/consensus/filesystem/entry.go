package filesystem

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LoadStorage initializes the storage layer by finding the latest log and snapshot.
func LoadStorage(dataDir string) (*Handler, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	snapshotFile, err := findLatestSnapshot(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to find latest snapshot file: %w", err)
	}

	file, diskTerm, diskIndex, err := findLatestLog(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to find latest log file: %w", err)
	}

	var currentSize int64
	var currentIndex = diskIndex
	var currentTerm = diskTerm
	recoveredEntries := make([]*LogEntry, 0)

	var snapshotEntries []*LogEntry
	if snapshotFile != nil {
		if _, err := snapshotFile.Seek(0, io.SeekStart); err == nil {
			entries, _, err := readLogEntries(snapshotFile)
			if err == nil && len(entries) > 0 {
				snapshotEntries = entries
			}
		}
	}

	if file != nil {
		entries, size, err := readLogEntries(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read log entries: %w", err)
		}

		recoveredEntries = entries
		currentSize = size

		// Update currentIndex and currentTerm to the ACTUAL end of the log file
		if len(recoveredEntries) > 0 {
			lastEntry := recoveredEntries[len(recoveredEntries)-1]
			currentIndex = lastEntry.Index
			currentTerm = lastEntry.Term
		}

		// reopen for append mode
		fileName := file.Name()
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to close file: %w", err)
		}

		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to reopen log file for appending: %w", err)
		}
	} else {
		if len(snapshotEntries) > 0 {
			lastSnap := snapshotEntries[len(snapshotEntries)-1]
			diskTerm = lastSnap.Term
			diskIndex = lastSnap.Index
			currentIndex = diskIndex
			currentTerm = diskTerm

			activeLogPath := filepath.Join(dataDir, fmt.Sprintf("%d-%d.log", diskTerm, diskIndex))
			file, err = os.OpenFile(activeLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, fmt.Errorf("failed to create active log file from snapshot: %w", err)
			}
		} else {
			// No log files and no snapshots exist, start fresh with the initial shard
			activeLogPath := filepath.Join(dataDir, "0-0.log")
			file, err = os.OpenFile(activeLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, fmt.Errorf("failed to create active log file: %w", err)
			}
		}
	}

	return &Handler{
		dataDir:          dataDir,
		activeFile:       file,
		snapshotFile:     snapshotFile,
		currentSize:      currentSize,
		prevTerm:         currentTerm,
		prevIndex:        currentIndex,
		discTerm:         diskTerm,
		discIndex:        diskIndex,
		entries:          recoveredEntries,
		SnapshotRequired: make(chan struct{}, 1),
	}, nil
}

func findLatestSnapshot(dataDir string) (*os.File, error) {
	snapshotPath := filepath.Join(dataDir, "active.log")
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No snapshot exists yet, start fresh
		}
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	return file, nil
}

// findLatestLog returns the file descriptor alongside the parsed Term and Index from its filename
func findLatestLog(dataDir string) (*os.File, int64, int64, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to read data directory: %w", err)
	}

	var latestTerm int64 = 0
	var latestIndex int64 = 0
	var latestFileName string
	var found bool

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		var term, index int64
		n, err := fmt.Sscanf(entry.Name(), "%d-%d.log", &term, &index)
		if err == nil && n == 2 {
			if !found || term > latestTerm || (term == latestTerm && index > latestIndex) {
				latestTerm = term
				latestIndex = index
				latestFileName = entry.Name()
				found = true
			}
		}
	}

	// No log files
	if !found {
		return nil, 0, 0, nil
	}

	logPath := filepath.Join(dataDir, latestFileName)
	file, err := os.OpenFile(logPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to open latest log file: %w", err)
	}

	return file, latestTerm, latestIndex, nil
}
