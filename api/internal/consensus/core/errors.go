package core

import "errors"

var (
	ErrLogBufferFull     = errors.New("log buffer is full")
	ErrAppendIndexBehind = errors.New("the index being appended is before the current index")
	ErrAppendIndexAhead  = errors.New("the index being appended has an index greater than the current index + 1")
	ErrAppendTermBehind  = errors.New("the term being appended is behind the current term")
	ErrCorruptedLogData  = errors.New("corrupted log data")
)
