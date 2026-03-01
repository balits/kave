package storage

import "unsafe"

type Batch interface {
	// Put and Delete record the mutations in the batch, but do not apply them to the underlying store until Commit is called
	Put(bucket Bucket, key, value []byte) error
	// Put and Delete record the mutations in the batch, but do not apply them to the underlying store until Commit is called
	Delete(bucket Bucket, key []byte) error
	// Commmit applies all recorded mutations to the underlying store atomically. After Commit is called, the batch is closed and cannot be used anymore.
	Commit() error
	// Abort discards all recorded mutations and closes the batch. After Abort is called, the batch cannot be used anymore.
	Abort() error
}

type deleteMarker struct{}

// WriteCollector helps to collect information
// about writes happening inside the batch,
// making it ideal to embed into other Batch implementations
type WriteCollector struct {
	puts    map[Bucket]map[string][]byte       // bucket -> key -> value
	deletes map[Bucket]map[string]deleteMarker // bucket -> key -> struct{} (just to track the keys, no need for values)
}

func NewWriteCollector() WriteCollector {
	return WriteCollector{
		puts:    make(map[Bucket]map[string][]byte),
		deletes: make(map[Bucket]map[string]deleteMarker),
	}
}

// RecordPut records a write mutation, and discards the previous delete mutation for the same key, if any
func (bc *WriteCollector) RecordPut(bucket Bucket, key, value []byte) {
	skey := unsafeBytesToString(key)
	if _, ok := bc.puts[bucket]; !ok {
		bc.puts[bucket] = make(map[string][]byte)
	}
	bc.puts[bucket][skey] = value

	// undo if in this batch it was already marked for deletion previously
	if deleteBucket, ok := bc.deletes[bucket]; ok {
		delete(deleteBucket, skey) // delete or noop
	}
}

// RecordDelete records a delete mutation, and discards the previous write mutation for the same key, if any
func (bc *WriteCollector) RecordDelete(bucket Bucket, key []byte) {
	skey := unsafeBytesToString(key)
	if _, ok := bc.deletes[bucket]; !ok {
		bc.deletes[bucket] = make(map[string]deleteMarker)
	}
	bc.deletes[bucket][skey] = deleteMarker{}
	// undo if in this batch it the key was already written
	if writeBucket, ok := bc.puts[bucket]; ok {
		delete(writeBucket, skey) // delete or noop
	}
}

func (bc *WriteCollector) Puts() map[Bucket]map[string][]byte {
	return bc.puts
}

func (bc *WriteCollector) Deletes() map[Bucket]map[string]deleteMarker {
	return bc.deletes
}

func (bc *WriteCollector) Reset() {
	clear(bc.deletes)
	clear(bc.puts)
}

func unsafeBytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}