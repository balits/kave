// store needs a raft instance
// fsm needs a store instance
// server needs a store instance
package store

import (
	"errors"
	"io"

	"github.com/balits/kave/internal/metrics"
	"github.com/hashicorp/raft"
)

type KV struct {
	Key   []byte
	Value []byte
}

type Bucket string

const (
	BucketKV    Bucket = "kv"    // the default bucket for key-value pairs
	BucketLease Bucket = "lease" // the bucket for lease-related data
)

var (
	ErrKeyNotFound    error = errors.New("key not found")
	ErrBucketNotFound error = errors.New("bucket not found")
	ErrBatchClosed    error = errors.New("batch already closed")
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

	Get(bucket Bucket, key []byte) (value []byte, err error)
	Set(bucket Bucket, key, value []byte) error
	Delete(bucket Bucket, key []byte) (value []byte, err error)

	PrefixScan(prefix []byte) ([]KV, error) // no bucket, only allowed on the default kv bucket
	NewBatch() (Batch, error)

	Snapshot() (raft.FSMSnapshot, error)
	Restore(io.ReadCloser) error

	Close() error
}
