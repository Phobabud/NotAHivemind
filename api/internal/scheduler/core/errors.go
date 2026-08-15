package core

import "errors"

var (
	ErrCASConcurrentConflict = errors.New("CAS concurrent conflict")
)
