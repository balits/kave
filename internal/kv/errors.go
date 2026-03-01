package kv

import "errors"

var (
	ErrCompacted = errors.New("compaction error: requested revision has been compacted away")
)
