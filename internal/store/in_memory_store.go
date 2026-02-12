package store

import (
	"encoding/gob"
	"errors"
	"io"
	"maps"
	"sync"

	"github.com/balits/thesis/internal/metrics"
	"github.com/balits/thesis/internal/util"
	"github.com/hashicorp/raft"
)

type InMemoryStore struct {
	mu             sync.RWMutex
	hashmap        map[string][]byte
	storageMetrics metrics.StorageMetricsAtomic
}

// NewInMemoryStore creates a new, empty in-memory key-value store
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		mu:      sync.RWMutex{},
		hashmap: make(map[string][]byte),
	}
}

// ========= store.KVStore impl =========

func (s *InMemoryStore) GetStale(key string) (value []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.hashmap[key]
	if !ok {
		err = ErrorKeyNotFound
	}
	s.storageMetrics.GetCount.Add(1)
	return
}

func (s *InMemoryStore) Set(key string, value []byte) (oldSize int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.hashmap[key]; ok {
		oldSize = len(old)
		s.storageMetrics.ByteSize.Add(uint64(len(value) - oldSize))
	} else {
		s.storageMetrics.ByteSize.Add(uint64(len(value)))
		s.storageMetrics.KeyCount.Add(1)
	}
	s.hashmap[key] = value
	s.storageMetrics.SetCount.Add(1)
	return
}

func (s *InMemoryStore) Delete(key string) (value []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.hashmap[key]
	if !ok {
		err = ErrorKeyNotFound // question: maybe move to noop on key not found
	}
	delete(s.hashmap, key)
	s.storageMetrics.DeleteCount.Add(1)
	util.AtomicSubNoUnderflow(&s.storageMetrics.KeyCount, 1)
	util.AtomicSubNoUnderflow(&s.storageMetrics.ByteSize, uint64(len(key)+len(value)))
	return
}

func (s *InMemoryStore) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := maps.Clone(s.hashmap)
	return InMemorySnapshot{data: data}, nil
}

func (s *InMemoryStore) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	var newmap map[string][]byte
	err := gob.NewDecoder(snapshot).Decode(&newmap)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.hashmap = newmap
	s.mu.Unlock()
	return nil
}

// InMemorySnapshot is a snapshot of the data at a given time, used to support log compaction and restore the FSM to a desired state.
// It is returned FSM.Snapshot() which itself shouldn't do heavy IO work.
// No mutex needed since snapshots are immutable
type InMemorySnapshot struct {
	data map[string][]byte
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s InMemorySnapshot) Persist(sink raft.SnapshotSink) error {
	err := gob.NewEncoder(sink).Encode(s.data)
	if err != nil {
		err2 := sink.Cancel()
		if err2 != nil {
			return errors.Join(err, err2)
		}
		return err
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (s InMemorySnapshot) Release() {
	//let gc clean it up later automatically
}

// ========= metrics.StorageMetricsProvider impl =========

func (s *InMemoryStore) StorageMetrics() *metrics.StorageMetrics {
	return s.storageMetrics.StorageMetrics()
}
