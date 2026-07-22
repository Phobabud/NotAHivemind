package filesystem

import (
	"ClusterManager/internal/consensus/core"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
)

type Payload struct {
	JobID    string          `json:"job_id"`
	State    Status          `json:"state"`
	Image    string          `json:"image"`
	Priority int             `json:"priority"`
	Payload  json.RawMessage `json:"payload"`
}

type Status int

const (
	Pending Status = iota
	Running
	Completed
	Failed
)

// encodeLogEntry packs a LogEntry into a length-prefixed binary byte slice.
// This is used by both the WAL appender and the Snapshot manager.
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

	// Calculate and verify minimum structural size
	// (8 bytes Index + 8 bytes Term + 2 bytes MasterIDLen = 18 bytes)
	if entrySize < 18 {
		return nil, core.ErrCorruptedLogData
	}

	// Verify the provided byte slice contains the full expected length-prefixed data
	if uint32(len(data)) < 4+entrySize {
		return nil, core.ErrCorruptedLogData
	}

	masterIDLen := binary.LittleEndian.Uint16(data[20:22])
	// Ensure the parsed masterID length doesn't overflow our overall entry limits
	if uint32(18+masterIDLen) > entrySize {
		return nil, core.ErrCorruptedLogData
	}
	masterID := string(data[22 : 22+masterIDLen])

	// Secure offset arithmetic and slice isolation for the payload
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

// createSnapshot uses the struct form of the payload to determine what makes it into a compressed snapshot
func createSnapshot(data []*LogEntry) []*LogEntry {
	snapshots := make(map[string]*LogEntry)
	excluded := make(map[string]bool)

	for i := len(data) - 1; i >= 0; i-- {
		var dataPoint Payload
		if err := json.Unmarshal(data[i].Payload, &dataPoint); err != nil {
			continue
		}
		jobID := dataPoint.JobID

		if excluded[jobID] {
			continue
		}

		if dataPoint.State == Completed || dataPoint.State == Failed {
			excluded[jobID] = true
			delete(snapshots, jobID)
			continue
		}

		if _, exists := snapshots[jobID]; !exists {
			snapshots[jobID] = data[i]
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
			break // Cleanly reached the end of the stream
		}
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read log header: %w", err)
		}

		entrySize := binary.LittleEndian.Uint32(header)
		buf := make([]byte, 4+entrySize)
		copy(buf[:4], header) // Preserve the header for the decoder

		if _, err := io.ReadFull(r, buf[4:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				break
			}
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
