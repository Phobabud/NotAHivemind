package core

import "errors"

// Sentinel errors for container lookup
var (
	ErrImageNotFound  = errors.New("no containers exist matching this image")
	ErrAllOccupied    = errors.New("matching containers exist, but all are currently occupied")
	ErrAlreadyPending = errors.New("job already pending")
	ErrAlreadyRunning = errors.New("job already running")
)
