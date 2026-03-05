// server needs a store instance
package bytestore

import (
	"io"

	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/storage"
)

// ByteStore is the key-value storage interface that
// needs to be implemented by all storage services (ephemeral or durable).
//
// It is considered the lowest level of abstraction in the project,
// only knowing about buckets and bytes, and not about higher level concepts like transactions, snapshots, etc.
// While not providing transactions in the MVCC sense, should provide atomic operations.
// Wether the store uses mutexes or internal transactions is up to the implementation.
//
// While not providing transactions in the strict sense, it does offer a Batch interface,
// which allows callers to group multiple operations together and commit them atomically.
//
// The store should only return ErrBucketNotFound or any errors wrapped in ErrInternalStorageError.

type ByteStore interface {
	metrics.StorageMetricsProvider

	// Get returns the value for the given bucket and key,
	// or an error if the bucket is not found.
	// If the key is not found, it returns nil value and no error.
	// The storage implementation may wrap any non bucket related errors into ErrInternalStorageError,
	// so that the caller can decide if it wants to retry or not based on the error type
	Get(bucket storage.Bucket, key []byte) (value []byte, err error)

	// Put sets the value for the given bucket and key.
	// If the bucket is not found, it returns an error.
	Put(bucket storage.Bucket, key, value []byte) error

	// TODO: remove return value
	Delete(bucket storage.Bucket, key []byte) (value []byte, err error)

	Scan(bucket storage.Bucket, f func(key, value []byte) bool) error
	PrefixScan(bucket storage.Bucket, prefix []byte, f func(key, value []byte) bool) error

	// ===== raft.FSM related methods =====
	// Snapshot() (raft.FSMSnapshot, error)
	// Restore(io.ReadCloser) error
	io.WriterTo
	io.ReaderFrom

	// ===== lifecycle methods =====
	Defragment() error
	Close() error

	// ====== batching ======
	NewBatch() (Batch, error)
}
