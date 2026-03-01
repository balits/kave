// server needs a store instance
package storage

import (
	"errors"
	"io"

	"github.com/balits/kave/internal/metrics"
)

const (
	COMPACTION_WINDOW     uint64 = 100
	COMPACTION_BATCH_SIZE int    = 1024
)

type RawKV struct {
	Key   []byte
	Value []byte
}

// Bucket represents a grouping of keys and values in the storage.
// Buckets are defined only at startup, and users are not able to modify them.
// These include the regular key-value bucket, but the lease bucket, and any other buckets that might be implemented in the future.
type Bucket string

var (
	// key not found is logically not an error
	// the returned value is just gonna be nil
	// ErrKeyNotFound    error = errors.New("key not found")

	// Error for when the bucket is not found
	// Buckets are set at startup, and users are not able to modify them
	// So the only way to get this error is if a wrong bucket is supplied to the storage functions
	ErrBucketNotFound       error = errors.New("storage error: bucket not found")
	ErrBatchClosed          error = errors.New("storage error: batch already closed")
	ErrInternalStorageError error = errors.New("storage error: internal storage error")
)

// Storage is the key-value storage interface that
// needs to be implemented by all storage services (ephemeral or durable).
//
// It serves two purpose, first are the common key-value operations like get, set or delete.
//
// Second, each Storage implementation should also implement a part of raft.FSM,
// specifically Snapshot() and Restore() that are both storage solution dependent
type Storage interface {
	metrics.StorageMetricsProvider

	// Get returns the value for the given bucket and key,
	// or an error if the bucket is not found.
	// If the key is not found, it returns nil value and no error.
	// The storage implementation may wrap any non bucket related errors into ErrInternalStorageError,
	// so that the caller can decide if it wants to retry or not based on the error type
	Get(bucket Bucket, key []byte) (value []byte, err error)

	// Put sets the value for the given bucket and key.
	// If the bucket is not found, it returns an error.
	Put(bucket Bucket, key, value []byte) error

	// TODO: remove return value
	Delete(bucket Bucket, key []byte) (value []byte, err error)

	Scan(bucket Bucket, f func(key, value []byte) bool) error
	PrefixScan(bucket Bucket, prefix []byte, f func(key, value []byte) bool) error

	// ===== raft.FSM related methods =====
	// Snapshot() (raft.FSMSnapshot, error)
	// Restore(io.ReadCloser) error
	io.WriterTo
	io.ReaderFrom

	// ===== lifecycle methods =====
	Compact(bucket Bucket, shouldDelete func(key []byte) bool) error
	Close() error

	Defrag() error

	// ====== batching ======
	NewBatch() (Batch, error)
}

// StorageKind represents the type of storage impl
type StorageKind string

const (
	StorageKindInMemory StorageKind = "inmemory"
	StorageKindBoltdb   StorageKind = "boltdb"
)

type StorageOptions struct {
	Path           string
	Kind           StorageKind
	InitialBuckets []Bucket
}
