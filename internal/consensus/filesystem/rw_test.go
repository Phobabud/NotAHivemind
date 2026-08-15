package filesystem

import (
	"NotAHiveMind/internal/consensus/core"
	"encoding/json"
	"os"
	"testing"
)

// setupTestHandler creates a pristine Handler instance with an isolated temporary file for each test
func setupTestHandler(t *testing.T) *Handler {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "active-test-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	t.Cleanup(func() {
		f.Close()
	})

	return &Handler{
		activeFile:       f,
		entries:          make([]*LogEntry, 0),
		SnapshotRequired: make(chan struct{}, 1),
		prevIndex:        0, // Raft indices start at 1, so prevIndex starts at 0
		prevTerm:         0,
		discIndex:        0,
		currentSize:      0,
	}
}

func TestAppend(t *testing.T) {
	// Successful append to disk/memory, rejection of out-of-order indices, and snapshot channel triggers.
	t.Run("Valid Sequence", func(t *testing.T) {
		h := setupTestHandler(t)

		entry1 := &LogEntry{Index: 1, Term: 1, SchedulerID: "s1", Payload: []byte("a")}
		if err := h.Append(entry1); err != nil {
			t.Fatalf("Unexpected error on valid append: %v", err)
		}

		if h.Index() != 1 || h.Term() != 1 || len(h.entries) != 1 {
			t.Errorf("Handler state not updated correctly after append")
		}
	})

	t.Run("Index Behind", func(t *testing.T) {
		h := setupTestHandler(t)
		_ = h.Append(&LogEntry{Index: 1, Term: 1})

		err := h.Append(&LogEntry{Index: 1, Term: 1}) // Duplicate index
		if err != core.ErrAppendIndexBehind {
			t.Errorf("Expected ErrAppendIndexBehind, got %v", err)
		}
	})

	t.Run("Index Ahead", func(t *testing.T) {
		h := setupTestHandler(t)
		_ = h.Append(&LogEntry{Index: 1, Term: 1})

		err := h.Append(&LogEntry{Index: 3, Term: 1}) // Skipped index 2
		if err != core.ErrAppendIndexAhead {
			t.Errorf("Expected ErrAppendIndexAhead, got %v", err)
		}
	})

	t.Run("Triggers Snapshot", func(t *testing.T) {
		h := setupTestHandler(t)

		// Artificially inflate the size to just below the 10MB limit
		h.currentSize = (10 * 1024 * 1024) - 10

		_ = h.Append(&LogEntry{Index: 1, Term: 1, Payload: []byte("trigger-the-limit")})

		select {
		case <-h.SnapshotRequired:
			// Success, channel received the signal
		default:
			t.Errorf("Expected SnapshotRequired channel to be triggered but it was empty")
		}
	})
}

func TestTruncateLog(t *testing.T) {
	// Proper removal of in-memory entries and file disk rewrite from a specified index.
	t.Run("Truncate Middle", func(t *testing.T) {
		h := setupTestHandler(t)

		_ = h.Append(&LogEntry{Index: 1, Term: 1, Payload: []byte("a")})
		_ = h.Append(&LogEntry{Index: 2, Term: 1, Payload: []byte("b")})
		_ = h.Append(&LogEntry{Index: 3, Term: 1, Payload: []byte("c")})

		if err := h.TruncateLog(2); err != nil {
			t.Fatalf("TruncateLog failed: %v", err)
		}

		if len(h.entries) != 1 {
			t.Fatalf("Expected 1 entry remaining, got %d", len(h.entries))
		}

		if h.Index() != 1 {
			t.Errorf("Expected prevIndex to be reverted to 1, got %d", h.Index())
		}
	})

	t.Run("Truncate Non-Existent Index", func(t *testing.T) {
		h := setupTestHandler(t)
		_ = h.Append(&LogEntry{Index: 1, Term: 1, Payload: []byte("a")})

		// Should safely do nothing and return no error
		if err := h.TruncateLog(99); err != nil {
			t.Fatalf("Unexpected error for non-existent index: %v", err)
		}
		if len(h.entries) != 1 {
			t.Errorf("Expected entries to remain untouched")
		}
	})
}

func TestEntries(t *testing.T) {
	// Correct slicing based on Raft index (accounting for discIndex offset) and out-of-bounds safety.
	h := setupTestHandler(t)
	for i := 1; i <= 5; i++ {
		_ = h.Append(&LogEntry{Index: int64(i), Term: 1, Payload: []byte("data")})
	}

	tests := []struct {
		name       string
		start      int64
		end        int64
		wantLength int
	}{
		{"Valid Range", 2, 4, 2},
		{"Out of Bounds Start", -1, 3, 2},
		{"Out of Bounds End", 4, 10, 2},
		{"Invalid Range", 5, 2, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.Entries(tt.start, tt.end)
			if len(result) != tt.wantLength {
				t.Errorf("Entries(%d, %d) returned %d items, want %d", tt.start, tt.end, len(result), tt.wantLength)
			}
		})
	}
}

func TestQueryEntries(t *testing.T) {
	// Filtering by schedulerID/payloadID, and returning ONLY the most recent state by iterating backwards.
	h := setupTestHandler(t)

	// Helper to create raw JSON payloads mimicking jobs/events
	makeJob := func(id string, state int) []byte {
		b, _ := json.Marshal(map[string]interface{}{"id": id, "state": state})
		return b
	}

	_ = h.Append(&LogEntry{Index: 1, SchedulerID: "sched-1", Payload: makeJob("job-1", 1)})
	_ = h.Append(&LogEntry{Index: 2, SchedulerID: "sched-2", Payload: makeJob("job-2", 1)})
	_ = h.Append(&LogEntry{Index: 3, SchedulerID: "sched-1", Payload: makeJob("job-1", 2)})

	t.Run("Query By Payload ID", func(t *testing.T) {
		targetID := "job-1"
		results := h.QueryEntries(nil, &targetID)

		if len(results) != 1 {
			t.Fatalf("Expected exactly 1 result (deduplicated), got %d", len(results))
		}
		if results[0].Index != 3 {
			t.Errorf("Expected to retrieve the most recent state (Index 3), got Index %d", results[0].Index)
		}
	})

	t.Run("Query By Scheduler ID", func(t *testing.T) {
		targetSched := "sched-2"
		results := h.QueryEntries(&targetSched, nil)

		if len(results) != 1 {
			t.Fatalf("Expected exactly 1 result for sched-2, got %d", len(results))
		}
		if results[0].Index != 2 {
			t.Errorf("Expected Index 2, got Index %d", results[0].Index)
		}
	})

	t.Run("Query Empty Inputs", func(t *testing.T) {
		results := h.QueryEntries(nil, nil)
		if results != nil {
			t.Errorf("Expected nil when providing no filters")
		}
	})
}
