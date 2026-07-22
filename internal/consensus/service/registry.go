package service

import "sync"

// requestRegistry coordinates asynchronous commit notifications.
type requestRegistry struct {
	mu       sync.Mutex
	channels map[string]chan bool
}

// newRequestRegistry initializes a ready-to-use registry.
func newRequestRegistry() *requestRegistry {
	return &requestRegistry{
		channels: make(map[string]chan bool),
	}
}

// Register reserves a buffered notification channel for a specific request ID.
func (r *requestRegistry) Register(requestID string) chan bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	ch := make(chan bool, 1)
	r.channels[requestID] = ch
	return ch
}

// Resolve delivers the consensus status to the waiting client and cleans up the entry.
func (r *requestRegistry) Resolve(requestID string, result bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ch, ok := r.channels[requestID]; ok {
		ch <- result
		delete(r.channels, requestID)
	}
}

// Deregister is called for manual cleanup (e.g., if a client times out).
func (r *requestRegistry) Deregister(requestID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.channels, requestID)
}
