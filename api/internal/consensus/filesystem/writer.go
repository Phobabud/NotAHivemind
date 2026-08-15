package filesystem

import (
	"NotAHiveMind/internal/consensus/core"
	"fmt"
	"io"
)

// Append safely writes a new log entry to disk and memory.
func (h *Handler) Append(entry *LogEntry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if entry.Index <= h.prevIndex {
		return core.ErrAppendIndexBehind
	}
	if entry.Index > h.prevIndex+1 {
		return core.ErrAppendIndexAhead
	}

	data := encodeLogEntry(entry)
	if _, err := h.activeFile.Write(data); err != nil {
		return fmt.Errorf("failed to write log entry to disk: %w", err)
	}

	if err := h.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync log to disk: %w", err)
	}

	h.entries = append(h.entries, entry)
	h.currentSize += int64(len(data))
	h.prevIndex = entry.Index
	h.prevTerm = entry.Term

	if h.currentSize >= h.maxLogSizekB*1024 {
		select {
		case h.SnapshotRequired <- struct{}{}:
		default:
		}
	}

	return nil
}

// TruncateLog removes all log entries starting from the given index to the end.
func (h *Handler) TruncateLog(fromIndex int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	cutIdx := -1
	for i, entry := range h.entries {
		if entry.Index == fromIndex {
			cutIdx = i
			break
		}
	}

	if cutIdx == -1 {
		return nil
	}

	h.entries = h.entries[:cutIdx]

	if err := h.activeFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate active log file: %w", err)
	}
	if _, err := h.activeFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek active log file: %w", err)
	}

	h.currentSize = 0
	for _, entry := range h.entries {
		buf := encodeLogEntry(entry)
		written, err := h.activeFile.Write(buf)
		if err != nil {
			return fmt.Errorf("failed to rewrite log entry during truncation: %w", err)
		}
		h.currentSize += int64(written)
	}

	if err := h.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync truncated log file: %w", err)
	}

	if len(h.entries) > 0 {
		lastEntry := h.entries[len(h.entries)-1]
		h.prevIndex = lastEntry.Index
		h.prevTerm = lastEntry.Term
	}

	return nil
}
