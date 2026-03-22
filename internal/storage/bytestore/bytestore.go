// server needs a store instance
package bytestore

import (
	"io"

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
// which allows callers to group multiple ops together and commit them atomically.
//
// The store should only return ErrBucketNotFound or any errors wrapped in ErrInternalStorageError.
type ByteStore interface {
	// ===== kv operations =====

	// Get returns the value for the given bucket and key,
	// or an error if the bucket is not found.
	// If the key is not found, it returns nil value and no error.
	// The storage implementation may wrap any non bucket related errors into ErrInternalStorageError,
	// so that the caller can decide if it wants to retry or not based on the error type
	Get(bucket storage.Bucket, key []byte) (value []byte, err error)

	// Put sets the value for the given bucket and key.
	// If the bucket is not found, it returns an error.
	Put(bucket storage.Bucket, key, value []byte) (old []byte, err error)

	// TODO: remove return value
	Delete(bucket storage.Bucket, key []byte) (old []byte, err error)

	Scan(bucket storage.Bucket, f func(key, value []byte) bool) error

	PrefixScan(bucket storage.Bucket, prefix []byte, f func(key, value []byte) bool) error

	// ===== raft.FSM related methods =====
	// Snapshot() (raft.FSMSnapshot, error)
	// Restore(io.ReadCloser) error
	// instead of coupling the lowest layer to the raft library
	// we use standard go interfaces instead
	io.WriterTo   // Snapshot
	io.ReaderFrom // Restore

	// ===== misc =====

	SizeBytes() int64
	Defragment() error
	Close() error
	Ping() error

	// ====== batching ======

	NewBatch() (Batch, error)
}
