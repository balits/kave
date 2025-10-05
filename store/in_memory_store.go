package store

import (
	"sync"

	"github.com/hashicorp/raft"
)

type InMemoryStore struct {
	mu      sync.RWMutex
	hashmap map[string]string

	raft *raft.Raft
}

// NewInMemoryStore creates a new, empty in-memory key-value store without a raft node instance.
// The raft node instance must be set throguh two-phase initialization later using SetRaftNode.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		mu:      sync.RWMutex{},
		hashmap: make(map[string]string),
		raft:    nil,
	}
}

func (s *InMemoryStore) SetRaftNode(raft *raft.Raft) {
	s.raft = raft
}

func (s *InMemoryStore) GetStale(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.hashmap[key]
	if !ok {
		return "", ErrorKeyNotFound
	}
	return value, nil
}

func (s *InMemoryStore) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hashmap[key] = value
	return nil
}

func (s *InMemoryStore) Delete(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.hashmap[key]
	if !ok {
		return "", nil
	}
	delete(s.hashmap, key)
	return value, nil
}

// Snapshot for the key value store that implements raft's FSMSnapshot
// It can save data from the store to persistent storage,
type InMemorySnapshot []byte

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s InMemorySnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s)
	if err != nil {
		return sink.Cancel()
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (s InMemorySnapshot) Release() {}
