package filesystem

import (
	"NotAHiveMind/internal/models" // Assuming standard import path based on your code
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func TestEncodeDecodeLogEntry(t *testing.T) {
	// Symmetric encoding/decoding behavior (data remains intact and is read properly)
	t.Run("Valid Entries", func(t *testing.T) {
		tests := []struct {
			name  string
			entry *LogEntry
		}{
			{
				name: "Standard entry",
				entry: &LogEntry{
					Index:       1,
					Term:        1,
					SchedulerID: "scheduler-1",
					Payload:     []byte(`{"id":"job-1", "state":1}`),
				},
			},
			{
				name: "Empty payload",
				entry: &LogEntry{
					Index:       2,
					Term:        1,
					SchedulerID: "scheduler-2",
					Payload:     []byte{},
				},
			},
			{
				name: "Empty scheduler ID",
				entry: &LogEntry{
					Index:       3,
					Term:        2,
					SchedulerID: "",
					Payload:     []byte(`{"entry":"ping"}`),
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				encoded := encodeLogEntry(tt.entry)
				decoded, err := decodeLogEntry(encoded)

				if err != nil {
					t.Fatalf("decodeLogEntry() returned unexpected error: %v", err)
				}

				if !reflect.DeepEqual(tt.entry, decoded) {
					t.Errorf("Decoded entry does not match original.\nGot: %+v\nWant: %+v", decoded, tt.entry)
				}
			})
		}
	})

	// Proper error handling when the binary data is malformed or truncated
	t.Run("Corrupted Data", func(t *testing.T) {
		tests := []struct {
			name string
			data []byte
		}{
			{"Nil data", nil},
			{"Header too small", []byte{0x01, 0x02}},
			{"Size claims large, payload missing", []byte{0xFF, 0x00, 0x00, 0x00, 0x01, 0x02}},
			{"Missing payload data", encodeLogEntry(&LogEntry{SchedulerID: "test"})[:10]}, // Truncated valid entry
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := decodeLogEntry(tt.data)
				if err == nil {
					t.Errorf("Expected an error for corrupted data, got nil")
				}
			})
		}
	})
}

func TestCreateSnapshot(t *testing.T) {
	// Helper to easily construct valid JSON payloads for the test table
	makeJobPayload := func(id string, state models.Status) []byte {
		payload := map[string]interface{}{"id": id, "state": state}
		b, _ := json.Marshal(payload)
		return b
	}
	makeSchedulerPayload := func(id string, entry string) []byte {
		payload := map[string]interface{}{"id": id, "entry": entry}
		b, _ := json.Marshal(payload)
		return b
	}

	tests := []struct {
		name     string
		input    []*LogEntry
		expected []int64 // We check against the expected LogEntry Indexes that should survive compaction
	}{
		{
			// A job updating multiple times should only keep its most recent state
			name: "Deduplicate Pending Jobs",
			input: []*LogEntry{
				{Index: 1, Payload: makeJobPayload("job-1", models.Pending)},
				{Index: 2, Payload: makeJobPayload("job-1", models.Running)},
			},
			expected: []int64{2},
		},
		{
			// Reaching a terminal state (Completed/Failed) should wipe the job entirely
			name: "Purge Completed Jobs",
			input: []*LogEntry{
				{Index: 1, Payload: makeJobPayload("job-1", models.Pending)},
				{Index: 2, Payload: makeJobPayload("job-1", models.Completed)},
				{Index: 3, Payload: makeJobPayload("job-2", models.Pending)}, // Unrelated job to ensure it survives
			},
			expected: []int64{3},
		},
		{
			// Schedulers spamming heartbeats should not have these heartbeats carried over
			name: "Deduplicate Scheduler Events",
			input: []*LogEntry{
				{Index: 1, Payload: makeSchedulerPayload("sched-1", "ping")},
				{Index: 2, Payload: makeSchedulerPayload("sched-1", "ping")},
				{Index: 3, Payload: makeSchedulerPayload("sched-2", "ping")},
			},
			expected: []int64{},
		},
		{
			// Entries that are completely invalid JSON should safely bypass compaction
			name: "Preserve Malformed JSON",
			input: []*LogEntry{
				{Index: 1, Payload: []byte(`{invalid-json`)},
				{Index: 2, Payload: makeJobPayload("job-1", models.Pending)},
			},
			expected: []int64{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := createSnapshot(tt.input)

			// The createSnapshot function uses a map internally, meaning the order of
			// the returned slice is random. We must sort it to test it deterministically.
			sort.Slice(result, func(i, j int) bool {
				return result[i].Index < result[j].Index
			})

			var actualIndexes []int64
			for _, entry := range result {
				actualIndexes = append(actualIndexes, entry.Index)
			}

			bothEmpty := len(actualIndexes) == len(tt.expected) && len(actualIndexes) == 0

			if !reflect.DeepEqual(actualIndexes, tt.expected) && !bothEmpty {
				t.Errorf("Compacted entries mismatch.\nGot indexes: %v\nWant indexes: %v", actualIndexes, tt.expected)
			}
		})
	}
}

func TestReadLogEntries(t *testing.T) {
	// The ability to continuously read multiple binary entries from a stream (like a file)
	t.Run("Read Multiple Entries", func(t *testing.T) {
		entry1 := &LogEntry{Index: 1, Term: 1, SchedulerID: "s1", Payload: []byte("payload1")}
		entry2 := &LogEntry{Index: 2, Term: 1, SchedulerID: "s1", Payload: []byte("payload2")}

		var stream bytes.Buffer
		stream.Write(encodeLogEntry(entry1))
		stream.Write(encodeLogEntry(entry2))

		entries, totalSize, err := readLogEntries(&stream)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(entries) != 2 {
			t.Fatalf("Expected 2 entries, got %d", len(entries))
		}

		if entries[0].Index != 1 || entries[1].Index != 2 {
			t.Errorf("Parsed entries do not match expected indexes")
		}

		// Ensure totalSize correctly tracked both headers and payloads
		expectedSize := int64(len(encodeLogEntry(entry1)) + len(encodeLogEntry(entry2)))
		if totalSize != expectedSize {
			t.Errorf("Expected totalSize %d, got %d", expectedSize, totalSize)
		}
	})

	// The stream gracefully handling an unexpected cut-off (e.g. server crash during write)
	t.Run("Handle Unexpected EOF", func(t *testing.T) {
		entry := &LogEntry{Index: 1, Term: 1, SchedulerID: "s1", Payload: []byte("payload1")}
		encoded := encodeLogEntry(entry)

		// Intentionally truncate the valid byte slice
		corruptedStream := bytes.NewReader(encoded[:len(encoded)-5])

		entries, _, err := readLogEntries(corruptedStream)

		if err == nil {
			t.Errorf("Expected an unexpected EOF error, got nil")
		}

		if len(entries) != 0 {
			t.Errorf("Should not have returned partial/corrupted entries")
		}
	})
}
