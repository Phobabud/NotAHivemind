package filesystem

import (
	"os"
	"sync"
)

// LogEntry represents a single operation or command in the Raft consensus log.
type LogEntry struct {
	Index       int64
	Term        int64
	SchedulerID string
	Payload     []byte
}

// Handler manages the thread-safe appending of logs to the physical disk,
// maintains the in-memory array, and handles streaming updates.
type Handler struct {
	mu           sync.RWMutex
	dataDir      string
	activeFile   *os.File
	snapshotFile *os.File
	currentSize  int64
	prevIndex    int64
	prevTerm     int64
	discIndex    int64
	discTerm     int64
	entries      []*LogEntry

	// SnapshotRequired is buffered. It alerts the Master cluster to initiate
	// compaction without blocking the active append thread.
	SnapshotRequired chan struct{}
}

func (h *Handler) Index() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.prevIndex
}

func (h *Handler) Term() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.prevTerm
}

func (h *Handler) UpdateTerm(term int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prevTerm = term
}

func (h *Handler) DiscIndex() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.discIndex
}

func (h *Handler) DiscTerm() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.discTerm
}

// Close releases resources and closes channels to stop background routines cleanly.
func (h *Handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.activeFile != nil {
		_ = h.activeFile.Close()
	}
	h.activeFile = nil

	if h.snapshotFile != nil {
		_ = h.snapshotFile.Close()
	}
	h.snapshotFile = nil

	if h.SnapshotRequired != nil {
		close(h.SnapshotRequired)
	}
	return nil
}
