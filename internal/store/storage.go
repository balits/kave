// store needs a raft instance
// fsm needs a store instance
// server needs a store instance
package store

import (
	"fmt"
	"io"

	"github.com/balits/thesis/internal/metrics"
	"github.com/hashicorp/raft"
)

type KVItem struct {
	Key   []byte
	Value []byte
}

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
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

	Get(key []byte) (value []byte, err error)
	Set(key, value []byte) error
	Delete(key []byte) (value []byte, err error)

	PrefixScan(prefix []byte) ([]KVItem, error)
	NewBatch() (Batch, error)

	Snapshot() (raft.FSMSnapshot, error)
	Restore(io.ReadCloser) error

	Close() error
}
