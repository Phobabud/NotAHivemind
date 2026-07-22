package filesystem

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
