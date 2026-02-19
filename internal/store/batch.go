package store

import "errors"

var ErrBatchClosed error = errors.New("batch already closed")

type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	Abort() error
}

// BatchCollector helps to collect information
// about mutations happening inside the batch,
// making it ideal to embedd into other Batch implementations
type BatchCollector struct {
	writes  map[string][]byte
	deletes map[string]struct{}
}

func NewBatchCollector() BatchCollector {
	return BatchCollector{
		writes:  make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}
}

// RecordSet records a write mutation, and discards the previous delete mutation for the same key, if any
func (bc *BatchCollector) RecordSet(key, value []byte) {
	bc.writes[string(key)] = value
	// undo if in this batch it was already marked for deletion previously
	delete(bc.deletes, string(key))
}

// RecordDelete records a delete mutation, and discards the previous write mutation for the same key, if any
func (bc *BatchCollector) RecordDelete(key []byte) {
	bc.deletes[string(key)] = struct{}{}
	// undo if in this batch it the key was already written
	delete(bc.writes, string(key))
}

func (bc *BatchCollector) Writes() map[string][]byte {
	return bc.writes
}

func (bc *BatchCollector) Deletes() map[string]struct{} {
	return bc.deletes
}

func (bc *BatchCollector) Reset() {
	clear(bc.deletes)
	clear(bc.writes)
}
