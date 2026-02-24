package inmem

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/store"
	"github.com/balits/kave/internal/util"
	"github.com/google/btree"
	"github.com/hashicorp/raft"
)

type Store struct {
	mu             sync.RWMutex
	buckets        map[store.Bucket]*btree.BTree
	storageMetrics metrics.StorageMetricsAtomic
}

// NewStore creates a new, empty in-memory key-value store
func NewStore() store.Storage {
	buckets := make(map[store.Bucket]*btree.BTree)
	buckets[store.BucketKV] = btree.New(BtreeDegreeDefault)
	buckets[store.BucketLease] = btree.New(BtreeDegreeDefault)

	return &Store{
		mu:             sync.RWMutex{},
		buckets:        buckets,
		storageMetrics: metrics.StorageMetricsAtomic{},
	}
}

// ========= store.KVStore impl =========

func (s *Store) Get(bucket store.Bucket, key []byte) (value []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doGet(bucket, key)
}

func (s *Store) Set(bucket store.Bucket, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doSet(bucket, key, value)
}

func (s *Store) Delete(bucket store.Bucket, key []byte) (value []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doDelete(bucket, key)
}

func (s *Store) PrefixScan(prefix []byte) ([]store.KV, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tree, ok := s.buckets[store.BucketKV]
	if !ok {
		return nil, store.ErrBucketNotFound
	}

	result := make([]store.KV, 0)

	tree.AscendGreaterOrEqual(KVBtreeItem{Key: prefix}, func(it btree.Item) bool {
		item := it.(KVBtreeItem)
		if bytes.HasPrefix(item.Key, prefix) {
			result = append(result, store.KV{Key: item.Key, Value: item.Value})
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
	clone := make(map[store.Bucket]*btree.BTree, len(s.buckets))
	for bucket, tree := range s.buckets {
		clone[bucket] = tree.Clone()
	}
	return Snapshot{clone}, nil
}

func (s *Store) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	tree, err := Decode(snapshot)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.buckets = tree
	s.mu.Unlock()
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tree := range s.buckets {
		tree.Clear(false)
	}
	return nil
}

// ========= metrics.StorageMetricsProvider impl =========

func (s *Store) StorageMetrics() *metrics.StorageMetrics {
	return s.storageMetrics.StorageMetrics()
}

// ========= kv internals =========
// these functions know nothing about transactions or locks
// they purely perform their logic and do some smetrics
func (s *Store) doGet(bucket store.Bucket, key []byte) ([]byte, error) {
	s.storageMetrics.GetCount.Add(1)

	tree, ok := s.buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("%w: %s", store.ErrBucketNotFound, string(bucket))
	}

	item := tree.Get(KVBtreeItem{Key: []byte(key)})
	if item == nil {
		return nil, fmt.Errorf("%w: %s", store.ErrKeyNotFound, string(key))
	}

	return item.(KVBtreeItem).Value, nil
}

func (s *Store) doSet(bucket store.Bucket, key, value []byte) error {
	s.storageMetrics.SetCount.Add(1)

	tree, ok := s.buckets[bucket]
	if !ok {
		return store.ErrBucketNotFound
	}

	oldItem := tree.ReplaceOrInsert(KVBtreeItem{key, value})

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

func (s *Store) doDelete(bucket store.Bucket, key []byte) ([]byte, error) {
	s.storageMetrics.DeleteCount.Add(1)

	tree, ok := s.buckets[bucket]
	if !ok {
		return nil, store.ErrBucketNotFound
	}

	oldItem := tree.Delete(KVBtreeItem{Key: key})
	if oldItem == nil {
		return nil, nil
	}

	value := oldItem.(KVBtreeItem).Value
	util.AtomicSubNoUnderflow(&s.storageMetrics.KeyCount, 1)
	util.AtomicSubNoUnderflow(&s.storageMetrics.ByteSize, uint64(len(key)+len(value)))
	return value, nil
}
