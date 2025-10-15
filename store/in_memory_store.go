package store

import (
	"encoding/gob"
	"io"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

type InMemoryStore struct {
	mu      sync.RWMutex
	hashmap map[string][]byte
}

// NewInMemoryStore creates a new, empty in-memory key-value store
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		mu:      sync.RWMutex{},
		hashmap: make(map[string][]byte),
	}
}

func (s *InMemoryStore) GetStale(key string) (value []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.hashmap[key]
	if !ok {
		err = ErrorKeyNotFound
	}
	return
}

func (s *InMemoryStore) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hashmap[key] = value
	return nil
}

func (s *InMemoryStore) Delete(key string) (value []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.hashmap[key]
	if !ok {
		err = ErrorKeyNotFound // question: maybe move to noop on key not found
	}
	delete(s.hashmap, key)
	return
}

func (s *InMemoryStore) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := maps.Clone(s.hashmap)
	return InMemorySnapshot{data: data}, nil
}

// Restore restores the stores data from a snapshot that has been encoded by the stores corresponding snapshot implementation, namely the function FSMSnapshot.Persist()
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

// InMemorySnapshot is a snapshot of the data at a given time, used to support log compactoin and restore the FSM to a desired state.
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
		return sink.Cancel()
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (s InMemorySnapshot) Release() {
	//let gc clean it up later automatically
}
