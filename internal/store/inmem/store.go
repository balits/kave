package inmem

import (
	"bytes"
	"io"
	"sync"

	"github.com/balits/thesis/internal/metrics"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/util"
	"github.com/google/btree"
	"github.com/hashicorp/raft"
)

type Store struct {
	mu             sync.RWMutex
	tree           *btree.BTree
	storageMetrics metrics.StorageMetricsAtomic
}

// NewStore creates a new, empty in-memory key-value store
func NewStore() store.Storage {
	return &Store{
		mu:             sync.RWMutex{},
		tree:           btree.New(BtreeDegreeDefault),
		storageMetrics: metrics.StorageMetricsAtomic{},
	}
}

// ========= store.KVStore impl =========

func (s *Store) GetStale(key []byte) (value []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doGet(key)
}

func (s *Store) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doSet(key, value)
}

func (s *Store) Delete(key []byte) (value []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doDelete(key)
}

func (s *Store) PrefixScan(prefix []byte) ([]store.KVItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]store.KVItem, 0)

	s.tree.AscendGreaterOrEqual(KVBtreeItem{Key: prefix}, func(it btree.Item) bool {
		item := it.(KVBtreeItem)
		if bytes.HasPrefix(item.Key, prefix) {
			result = append(result, store.KVItem{Key: item.Key, Value: item.Value})
		}
		return true
	})

	return result, nil
}

func (s *Store) NewBatch() (store.Batch, error) {
	return newBatch(s), nil
}

// ========= impl rafts storage specific operations =========

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Snapshot{s.tree.Clone()}, nil
}

func (s *Store) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	tree, err := Decode(snapshot)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.tree = tree
	s.mu.Unlock()
	return nil
}

func (s *Store) Close() error {
	s.tree = nil
	return nil
}

// ========= metrics.StorageMetricsProvider impl =========

func (s *Store) StorageMetrics() *metrics.StorageMetrics {
	return s.storageMetrics.StorageMetrics()
}

// ========= kv internals =========
// these functions no nothing about transactions or locks
// they purely perform their logic and do some smetrics
func (s *Store) doGet(key []byte) (value []byte, err error) {
	s.storageMetrics.GetCount.Add(1)

	item := s.tree.Get(KVBtreeItem{Key: []byte(key)})
	if item == nil {
		return nil, store.ErrKeyNotFound
	}

	value = item.(KVBtreeItem).Value
	return
}

func (s *Store) doSet(key, value []byte) error {
	s.storageMetrics.SetCount.Add(1)

	oldItem := s.tree.ReplaceOrInsert(KVBtreeItem{key, value})

	if oldItem != nil {
		oldValue := oldItem.(KVBtreeItem).Value
		oldSize := len(oldValue)
		s.storageMetrics.ByteSize.Add(uint64(len(value) - oldSize))
	} else {
		s.storageMetrics.ByteSize.Add(uint64(len(value) + len(key)))
		s.storageMetrics.KeyCount.Add(1)
	}

	return nil
}

func (s *Store) doDelete(key []byte) (value []byte, err error) {
	s.storageMetrics.DeleteCount.Add(1)

	oldItem := s.tree.Delete(KVBtreeItem{Key: key})
	if oldItem == nil {
		return nil, nil
	}

	value = oldItem.(KVBtreeItem).Value
	util.AtomicSubNoUnderflow(&s.storageMetrics.KeyCount, 1)
	util.AtomicSubNoUnderflow(&s.storageMetrics.ByteSize, uint64(len(key)+len(value)))
	return
}
