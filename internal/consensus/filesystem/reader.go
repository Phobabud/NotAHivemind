package filesystem

import "encoding/json"

// Entries securely returns a copy of the slice of entries in the given Raft index range.
func (h *Handler) Entries(startRaftIndex int64, endRaftIndex int64) []*LogEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	startSlice := int(startRaftIndex - h.discIndex - 1)
	endSlice := int(endRaftIndex - h.discIndex - 1)

	if startSlice < 0 {
		startSlice = 0
	}
	if endSlice < 0 {
		endSlice = 0
	}
	if endSlice > len(h.entries) {
		endSlice = len(h.entries)
	}
	if startSlice >= endSlice {
		return nil
	}

	result := make([]*LogEntry, endSlice-startSlice)
	copy(result, h.entries[startSlice:endSlice])
	return result
}

// QueryEntries flexibly searches the log based on provided filters. It always returns the most recent state.
func (h *Handler) QueryEntries(schedulerID *string, payloadID *string) []*LogEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if schedulerID == nil && payloadID == nil {
		return nil
	}

	var results []*LogEntry
	foundPayloads := make(map[string]bool)

	// Iterate backwards to get the latest states first
	for i := len(h.entries) - 1; i >= 0; i-- {
		entry := h.entries[i]

		if schedulerID != nil && entry.SchedulerID != *schedulerID {
			continue
		}

		var peek genericPayload
		if err := json.Unmarshal(entry.Payload, &peek); err != nil {
			continue
		}

		if payloadID != nil && peek.ID != *payloadID {
			continue
		}

		if !foundPayloads[peek.ID] {
			foundPayloads[peek.ID] = true
			results = append(results, entry)
			if payloadID != nil {
				break
			}
		}
	}

	return results
}
