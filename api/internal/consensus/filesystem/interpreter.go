package filesystem

import (
	"NotAHiveMind/internal/consensus/core"
	"NotAHiveMind/internal/models"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
)

// genericPayload helps us peek at the JSON to determine its type without unmarshaling the whole thing.
type genericPayload struct {
	ID    string         `json:"id"`
	State *models.Status `json:"state,omitempty"` // If State is present, it's a Job.
	Entry *string        `json:"entry,omitempty"` // If Entry is present, it's a Scheduler event.
}

// encodeLogEntry packs a LogEntry into a length-prefixed binary byte slice.
func encodeLogEntry(entry *LogEntry) []byte {
	masterIDLen := len(entry.SchedulerID)

	// entrySize + (8 bytes Index + 8 bytes Term + 2 bytes MasterID Length) + masterID + payload
	entrySize := 8 + 8 + 2 + masterIDLen + len(entry.Payload)
	buf := make([]byte, 4+entrySize)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(entrySize))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(entry.Index))
	binary.LittleEndian.PutUint64(buf[12:20], uint64(entry.Term))

	binary.LittleEndian.PutUint16(buf[20:22], uint16(masterIDLen))
	copy(buf[22:22+masterIDLen], entry.SchedulerID)

	copy(buf[22+masterIDLen:], entry.Payload)

	return buf
}

// decodeLogEntry takes the bytes encoded by encodeLogEntry and decodes them
func decodeLogEntry(data []byte) (*LogEntry, error) {
	if len(data) < 4 {
		return nil, core.ErrCorruptedLogData
	}

	entrySize := binary.LittleEndian.Uint32(data[0:4])

	if entrySize < 18 {
		return nil, core.ErrCorruptedLogData
	}

	if uint32(len(data)) < 4+entrySize {
		return nil, core.ErrCorruptedLogData
	}

	masterIDLen := binary.LittleEndian.Uint16(data[20:22])
	if uint32(18+masterIDLen) > entrySize {
		return nil, core.ErrCorruptedLogData
	}
	masterID := string(data[22 : 22+masterIDLen])

	payloadOffset := 22 + int(masterIDLen)
	payloadSize := int(entrySize) - (18 + int(masterIDLen))

	entry := &LogEntry{
		Index:       int64(binary.LittleEndian.Uint64(data[4:12])),
		Term:        int64(binary.LittleEndian.Uint64(data[12:20])),
		SchedulerID: masterID,
		Payload:     make([]byte, payloadSize),
	}
	copy(entry.Payload, data[payloadOffset:payloadOffset+payloadSize])

	return entry, nil
}

// createSnapshot analyzes payloads to discard completed jobs and duplicate entries, creating a compressed log state.
func createSnapshot(data []*LogEntry) []*LogEntry {
	snapshots := make(map[string]*LogEntry)
	excluded := make(map[string]bool)

	// Iterate backwards to always find the most recent state of a specific ID first
	for i := len(data) - 1; i >= 0; i-- {
		var peek genericPayload
		if err := json.Unmarshal(data[i].Payload, &peek); err != nil {
			// If it's malformed, keep it in the snapshot for the state machine to handle/reject
			snapshots[fmt.Sprintf("raw-%d", data[i].Index)] = data[i]
			continue
		}

		id := peek.ID
		if id == "" {
			continue // Skip empty IDs to avoid key collisions
		}

		if excluded[id] {
			continue
		}

		// JobPayload
		if peek.State != nil {
			if *peek.State == models.Completed || *peek.State == models.Failed {
				excluded[id] = true
				delete(snapshots, id) // Purge from map if we found a prior pending state
				continue
			}

			// Only keep the most recent pending/running state for this job ID
			if _, exists := snapshots[id]; !exists {
				snapshots[id] = data[i]
			}
			continue
		}

		// SchedulerPayload
		if peek.Entry != nil {
			// Explicitly ignore scheduler payloads
			continue
		}
	}

	return slices.Collect(maps.Values(snapshots))
}

// readLogEntries returns a slice of LogEntries decoded from an io.Reader
func readLogEntries(r io.Reader) ([]*LogEntry, int64, error) {
	var recoveredEntries []*LogEntry
	var totalSize int64

	for {
		header := make([]byte, 4)
		_, err := io.ReadFull(r, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read log header: %w", err)
		}

		entrySize := binary.LittleEndian.Uint32(header)
		buf := make([]byte, 4+entrySize)
		copy(buf[:4], header)

		if _, err := io.ReadFull(r, buf[4:]); err != nil {
			return nil, 0, fmt.Errorf("failed to read log payload (corrupted stream?): %w", err)
		}

		entry, err := decodeLogEntry(buf)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode log entry: %w", err)
		}

		recoveredEntries = append(recoveredEntries, entry)
		totalSize += int64(4 + entrySize)
	}

	return recoveredEntries, totalSize, nil
}
