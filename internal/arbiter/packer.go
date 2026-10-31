// Package arbiter provides generalized scheduling and packing utilities.
package arbiter

import (
	"sort"
	"sync"
)

// Queue is a thread-safe, generalized priority queue designed for bin-packing.
// It uses accessor functions to extract necessary fields, bypassing Go's
// inability to constrain generic types by struct fields.
type Queue[T any] struct {
	items map[string]T
	order []T
	dirty bool
	mu    sync.RWMutex

	idFn   func(T) string
	prioFn func(T) int64
	cpuFn  func(T) int64
	memFn  func(T) int64
}

// NewQueue initializes and returns a new generalized packing queue.
// It requires accessor functions to extract the ID, Priority, CPU, and Memory
// requirements from the generic type.
func NewQueue[T any](idFn func(T) string, prioFn func(T) int64, cpuFn func(T) int64, memFn func(T) int64) *Queue[T] {
	return &Queue[T]{
		items:  make(map[string]T),
		order:  make([]T, 0),
		dirty:  false,
		idFn:   idFn,
		prioFn: prioFn,
		cpuFn:  cpuFn,
		memFn:  memFn,
	}
}

// Push adds a new item to the queue. If an item with the same ID already exists,
// it is overwritten.
func (q *Queue[T]) Push(item T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := q.idFn(item)

	// If it's a new item, append it to the slice and mark as dirty for resorting.
	if _, exists := q.items[id]; !exists {
		q.order = append(q.order, item)
		q.dirty = true
	}

	q.items[id] = item
}

// PushBatch adds multiple items to the queue in a single atomic operation.
// This is significantly faster than calling Push() in a loop because it only
// acquires the sync.RWMutex lock once.
func (q *Queue[T]) PushBatch(items []T) {
	if len(items) == 0 {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, item := range items {
		id := q.idFn(item)
		if _, exists := q.items[id]; !exists {
			q.order = append(q.order, item)
			q.dirty = true
		}
		q.items[id] = item
	}
}

// PeekForHeuristic returns the number of highest-priority items from the queue without removing them.
func (q *Queue[T]) PeekForHeuristic(heuristic func(T) bool) []T {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if len(q.items) == 0 {
		return nil
	}

	var result []T
	for _, v := range q.items {
		if heuristic(v) {
			result = append(result, v)
		}
	}

	return result
}

// RemoveByID safely deletes an item from the queue in O(1) time.
// The actual slice compaction is deferred (lazy deletion) to save CPU cycles.
func (q *Queue[T]) RemoveByID(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.items[id]; exists {
		delete(q.items, id)
		q.dirty = true // Triggers a compaction on the next pop
	}
}

// Clear empties the queue of all items, resetting its state.
func (q *Queue[T]) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = make(map[string]T)
	q.order = make([]T, 0)
	q.dirty = false
}

// IDs returns a slice of all IDs currently in the queue.
// This is useful for dashboard monitoring or full-queue synchronization.
func (q *Queue[T]) IDs() []string {
	q.mu.RLock()
	defer q.mu.RUnlock()

	ids := make([]string, 0, len(q.items))
	for id := range q.items {
		ids = append(ids, id)
	}
	return ids
}

// Size returns the exact number of active items currently in the queue.
func (q *Queue[T]) Size() int32 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return int32(len(q.items))
}

// Items returns a slice of all items currently in the queue.
func (q *Queue[T]) Items() []T {
	q.mu.RLock()
	defer q.mu.RUnlock()

	items := make([]T, 0, len(q.items))
	for _, item := range q.items {
		items = append(items, item)
	}
	return items
}

// PopBestFit iterates from the highest priority downwards, returning and removing
// the first item that fits within the provided CPU and Memory limits.
func (q *Queue[T]) PopBestFit(availCPU, availMem int64) (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		var zero T
		return zero, false
	}

	if q.dirty {
		q.sortAndCompact()
	}

	for i, item := range q.order {
		if q.cpuFn(item) <= availCPU && q.memFn(item) <= availMem {
			// Found the highest priority item that fits the constraints
			delete(q.items, q.idFn(item))

			// Remove the item from the slice in-place
			q.order = append(q.order[:i], q.order[i+1:]...)
			return item, true
		}
	}

	var zero T
	return zero, false
}

// PopBestFitHeuristic is similar to PopBestFit but allows for an additional heuristic function to filter items.
func (q *Queue[T]) PopBestFitHeuristic(availCPU, availMem int64, heuristic func(T) bool) (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		var zero T
		return zero, false
	}

	if q.dirty {
		q.sortAndCompact()
	}

	var bestIndex int = -1
	for i, item := range q.order {
		if q.cpuFn(item) <= availCPU && q.memFn(item) <= availMem && heuristic(item) {
			bestIndex = i
			break
		}
	}

	if bestIndex == -1 {
		var zero T
		return zero, false
	}

	bestItem := q.order[bestIndex]
	delete(q.items, q.idFn(bestItem))
	q.order = append(q.order[:bestIndex], q.order[bestIndex+1:]...)
	return bestItem, true
}

// sortAndCompact removes deleted items from the backing slice and sorts it.
// It prioritizes Priority (Highest First), then Memory (Largest First).
// The caller MUST hold the write lock (q.mu.Lock()) before calling this.
func (q *Queue[T]) sortAndCompact() {
	compacted := make([]T, 0, len(q.items))
	for _, item := range q.order {
		if _, exists := q.items[q.idFn(item)]; exists {
			compacted = append(compacted, item)
		}
	}
	q.order = compacted

	sort.SliceStable(q.order, func(i, j int) bool {
		prioI := q.prioFn(q.order[i])
		prioJ := q.prioFn(q.order[j])

		if prioI != prioJ {
			return prioI > prioJ
		}
		// Tie-breaker: Pack largest memory items first to prevent cluster fragmentation
		return q.memFn(q.order[i]) > q.memFn(q.order[j])
	})

	q.dirty = false
}
