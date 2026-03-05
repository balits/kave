package bytestore

import (
	"unsafe"

	"github.com/balits/kave/internal/storage"
)

// WriteCollector helps to collect information
// about writes happening inside the batch,
// making it ideal to embed into other Batch implementations
type WriteCollector struct {
	puts    map[storage.Bucket]map[string][]byte   // bucket -> key -> value
	deletes map[storage.Bucket]map[string]struct{} // bucket -> key -> struct{} (just to track the keys, no need for values)
}

func NewWriteCollector() WriteCollector {
	return WriteCollector{
		puts:    make(map[storage.Bucket]map[string][]byte),
		deletes: make(map[storage.Bucket]map[string]struct{}),
	}
}

// RecordPut records a write mutation, and discards the previous delete mutation for the same key, if any
func (bc *WriteCollector) RecordPut(bucket storage.Bucket, key, value []byte) {
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
func (bc *WriteCollector) RecordDelete(bucket storage.Bucket, key []byte) {
	skey := unsafeBytesToString(key)
	if _, ok := bc.deletes[bucket]; !ok {
		bc.deletes[bucket] = make(map[string]struct{})
	}
	bc.deletes[bucket][skey] = struct{}{}
	// undo if in this batch it the key was already written
	if writeBucket, ok := bc.puts[bucket]; ok {
		delete(writeBucket, skey) // delete or noop
	}
}

func (bc *WriteCollector) Puts() map[storage.Bucket]map[string][]byte {
	return bc.puts
}

func (bc *WriteCollector) Deletes() map[storage.Bucket]map[string]struct{} {
	return bc.deletes
}

func (bc *WriteCollector) Reset() {
	clear(bc.deletes)
	clear(bc.puts)
}

func unsafeBytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
