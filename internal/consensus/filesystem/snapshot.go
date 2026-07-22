package filesystem

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// StoreSnapshot should run as a goroutine.
func (h *Handler) StoreSnapshot() error {
	for range h.SnapshotRequired {
		if h.snapshotFile == nil {
			return fmt.Errorf("snapshot file is not initialized")
		}
		h.mu.Lock()
		if len(h.entries) == 0 {
			h.mu.Unlock()
			continue
		}
		compressedData := createSnapshot(h.entries)
		snapshotCutoffIndex := h.entries[len(h.entries)-1].Index
		lastTerm := h.entries[len(h.entries)-1].Term
		lastIndex := h.entries[len(h.entries)-1].Index
		h.mu.Unlock()

		_ = h.snapshotFile.Close()
		h.snapshotFile = nil

		snapPath := filepath.Join(h.dataDir, "active.log")
		snapFile, err := os.OpenFile(snapPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("failed to open snapshot file for writing: %w", err)
		}
		h.snapshotFile = snapFile

		for _, entry := range compressedData {
			if _, err := h.snapshotFile.Write(encodeLogEntry(entry)); err != nil {
				_ = snapFile.Close()
				return fmt.Errorf("failed to write snapshot entry: %w", err)
			}
		}

		if err := h.snapshotFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync snapshot file: %w", err)
		}

		newLogPath := filepath.Join(h.dataDir, fmt.Sprintf("%d-%d.log", lastTerm, lastIndex))
		newFile, err := os.OpenFile(newLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to reopen log file for appending: %w", err)
		}

		if err := h.rotateFile(newFile, snapshotCutoffIndex, lastTerm, lastIndex); err != nil {
			return err
		}
	}
	return nil
}

// rotateFile is called by the Snapshot manager to safely swap the active log file under lock.
func (h *Handler) rotateFile(newFile *os.File, snapshotIndex int64, lastTerm int64, lastIndex int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.activeFile.Close(); err != nil {
		return fmt.Errorf("failed to close old log file: %w", err)
	}

	h.activeFile = newFile
	h.currentSize = 0

	remainingEntries := make([]*LogEntry, 0)
	for _, entry := range h.entries {
		if entry.Index > snapshotIndex {
			remainingEntries = append(remainingEntries, entry)

			// Write outstanding in-flight logs directly to the new physical WAL on disk
			buf := encodeLogEntry(entry)
			written, err := h.activeFile.Write(buf)
			if err != nil {
				return fmt.Errorf("failed to persist active remaining entries to new log: %w", err)
			}
			h.currentSize += int64(written)
		}
	}
	h.entries = remainingEntries

	// Guarantee crash-survival of in-flight logs
	if h.currentSize > 0 {
		if err := h.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync active remaining entries during rotation: %w", err)
		}
	}

	h.discIndex = lastIndex
	h.discTerm = lastTerm

	return nil
}

// SnapshotReader opens a safe, read-only isolated handle to the active.log snapshot file.
func (h *Handler) SnapshotReader() (io.ReadCloser, int64, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.snapshotFile == nil {
		return nil, 0, fmt.Errorf("no snapshot file initialized")
	}

	stat, err := h.snapshotFile.Stat()
	if err != nil {
		return nil, 0, err
	}

	file, err := os.OpenFile(h.snapshotFile.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, err
	}

	return file, stat.Size(), nil
}

// InstallSnapshot writes incoming raw binary chunks into snapshot.tmp and then moves the file to active.log to avoid oopsies
func (h *Handler) InstallSnapshot(offset int64, data []byte, done bool, lastIncludedIndex int64, lastIncludedTerm int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	tempPath := filepath.Join(h.dataDir, "snapshot.tmp")

	var file *os.File
	var err error
	if offset == 0 {
		file, err = os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	} else {
		file, err = os.OpenFile(tempPath, os.O_WRONLY, 0644)
	}
	if err != nil {
		return fmt.Errorf("failed to open snapshot temp file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write snapshot chunk at offset %d: %w", offset, err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot chunk: %w", err)
	}

	if done {
		if h.snapshotFile != nil {
			_ = h.snapshotFile.Close()
		}
		h.snapshotFile = nil

		snapPath := filepath.Join(h.dataDir, "active.log")
		if err := os.Rename(tempPath, snapPath); err != nil {
			return fmt.Errorf("failed to finalize snapshot installation: %w", err)
		}

		h.snapshotFile, err = os.OpenFile(snapPath, os.O_RDONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to reopen snapshot file: %w", err)
		}

		if h.activeFile != nil {
			_ = h.activeFile.Truncate(0)
			_, _ = h.activeFile.Seek(0, io.SeekStart)
		}

		h.entries = make([]*LogEntry, 0)
		h.currentSize = 0

		h.prevIndex = lastIncludedIndex
		h.prevTerm = lastIncludedTerm
		h.discIndex = lastIncludedIndex
		h.discTerm = lastIncludedTerm
	}

	return nil
}
