package kv

import (
	"errors"
)

var (
	// ErrCompacted is returned when a requested revision has been compacted away
	// and is no longer available.
	ErrCompacted = errors.New("revision has been compacted away")

	// ErrKeyNotFound is returned when a requested key is not found in the store.
	ErrKeyNotFound = errors.New("key not found")
)
