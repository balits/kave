package kv

import (
	"errors"
)

var (
	ErrCompacted   = errors.New("revision has been compacted away")
	ErrKeyNotFound = errors.New("key not found")
)
