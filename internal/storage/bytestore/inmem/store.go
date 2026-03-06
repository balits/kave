package inmem

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	"github.com/balits/kave/internal/util"
	"github.com/google/btree"
)

type InmemStore struct {
	rwlock         sync.RWMutex
	buckets        map[storage.Bucket]*btree.BTree
	storageMetrics *metrics.StorageMetricsAtomic
}

// NewStore creates a new, empty in-memory key-value store
func NewStore(opts storage.StorageOptions) bytestore.ByteStore {
	if opts.Kind != storage.StorageKindInMemory {
		panic("failed to create inmem store: param opts.Kind was not StorageKindInMemory")
	}

	buckets := make(map[storage.Bucket]*btree.BTree)
	for _, bucket := range opts.InitialBuckets {
		buckets[bucket] = btree.New(BtreeDegreeDefault)
	}

	return &InmemStore{
		rwlock:         sync.RWMutex{},
		buckets:        buckets,
		storageMetrics: &metrics.StorageMetricsAtomic{},
	}
}

// ========= storage.KVStore impl =========

func (s *InmemStore) Get(bucket storage.Bucket, key []byte) (value []byte, err error) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.doGet(bucket, key)
}

func (s *InmemStore) Put(bucket storage.Bucket, key, value []byte) error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	return s.doSet(bucket, key, value)
}

func (s *InmemStore) Delete(bucket storage.Bucket, key []byte) (value []byte, err error) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	return s.doDelete(bucket, key)
}

func (s *InmemStore) PrefixScan(bucket storage.Bucket, prefix []byte, f func(key, value []byte) bool) error {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	tree, ok := s.buckets[bucket]
	if !ok {
		return storage.ErrBucketNotFound
	}

	tree.AscendGreaterOrEqual(KVBtreeItem{Key: prefix}, func(it btree.Item) bool {
		item := it.(KVBtreeItem)
		if bytes.HasPrefix(item.Key, prefix) {
			return f(item.Key, item.Value)
		}
		return false
	})

	return nil
}

func (s *InmemStore) Scan(bucket storage.Bucket, op func(key, value []byte) bool) error {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	tree, ok := s.buckets[bucket]
	if !ok {
		return storage.ErrBucketNotFound
	}

	tree.Ascend(func(it btree.Item) bool {
		item := it.(KVBtreeItem)
		return op(item.Key, item.Value)
	})
	return nil
}

func (s *InmemStore) NewBatch() (bytestore.Batch, error) {
	return newBatch(s), nil
}

// ========= impl WriterTo, ReaderFrom for rafts snapshotting and restoring =========

func (s *InmemStore) WriteTo(w io.Writer) (int64, error) {
	s.rwlock.RLock()
	clone := make(map[storage.Bucket]*btree.BTree, len(s.buckets))
	for bucket, tree := range s.buckets {
		clone[bucket] = tree.Clone()
	}
	defer s.rwlock.RUnlock()

	if err := Encode(w, s); err != nil {
		return 0, err
	}
	return 0, nil // we dont really know how many bytes were written, and it doesnt matter for us, so we just return 0
}

func (s *InmemStore) ReadFrom(r io.Reader) (int64, error) {
	tree, err := Decode(r)
	if err != nil {
		return 0, err
	}

	s.rwlock.Lock()
	s.buckets = tree
	s.rwlock.Unlock()
	return 0, nil // we dont really know how many bytes were read, and it doesnt matter for us, so we just return 0
}

func (s *InmemStore) Defragment() error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	//TODO build new tree after iterating
	return nil
}

// Simple, dumb compaction logic that should be improved upon later, but its fine now
// func (s *InmemStore) Compact(bucket storage.Bucket, shouldDelete func([]byte) bool) error {
// 	tree, ok := s.buckets[bucket]
// 	if !ok {
// 		return storage.ErrBucketNotFound
// 	}

// 	s.rwlock.Lock()
// 	defer s.rwlock.Unlock()

// 	batch := make([]KVBtreeItem, 0, storage.COMPACTION_BATCH_SIZE)

// 	for {
// 		tree.Ascend(func(bi btree.Item) bool {
// 			item := bi.(KVBtreeItem)
// 			if shouldDelete(item.Key) {
// 				batch = append(batch, item)
// 				if len(batch) == storage.COMPACTION_BATCH_SIZE {
// 					return false
// 				}
// 			}
// 			return true
// 		})

// 		if len(batch) == 0 {
// 			break
// 		}

// 		for _, kv := range batch {
// 			tree.Delete(kv)
// 		}
// 		batch = batch[:0]
// 	}

// 	return nil
// }

func (s *InmemStore) Close() error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	for _, tree := range s.buckets {
		tree.Clear(false)
	}
	return nil
}

// ========= metrics.StorageMetricsProvider impl =========

func (s *InmemStore) StorageMetrics() *metrics.StorageMetrics {
	return s.storageMetrics.StorageMetrics()
}

// ========= kv internals =========
// these functions know nothing about transactions or locks
// they purely perform their logic and do some smetrics
// which is preferred in batches, since they should only lock/unlock once for the whole batch

func (s *InmemStore) doGet(bucket storage.Bucket, key []byte) ([]byte, error) {
	s.storageMetrics.GetCount.Add(1)

	tree, ok := s.buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("%w: %s", storage.ErrBucketNotFound, string(bucket))
	}

	item := tree.Get(KVBtreeItem{Key: []byte(key)})
	if item == nil {
		return nil, nil
	}

	return item.(KVBtreeItem).Value, nil
}

func (s *InmemStore) doSet(bucket storage.Bucket, key, value []byte) error {
	s.storageMetrics.SetCount.Add(1)

	if len(key) == 0 {
		return storage.ErrEmptyKey
	}

	tree, ok := s.buckets[bucket]
	if !ok {
		return storage.ErrBucketNotFound
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

func (s *InmemStore) doDelete(bucket storage.Bucket, key []byte) ([]byte, error) {
	s.storageMetrics.DeleteCount.Add(1)

	tree, ok := s.buckets[bucket]
	if !ok {
		return nil, storage.ErrBucketNotFound
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

// TODO: somehow we need to snapshot storage metrics too
func clone(s *InmemStore) *InmemStore {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	bucks := make(map[storage.Bucket]*btree.BTree, len(s.buckets))
	for bucket, tree := range s.buckets {
		bucks[bucket] = tree.Clone()
	}

	newMetrics := new(metrics.StorageMetricsAtomic)
	newMetrics.SetCount.Store(s.storageMetrics.SetCount.Load())
	newMetrics.GetCount.Store(s.storageMetrics.GetCount.Load())
	newMetrics.DeleteCount.Store(s.storageMetrics.DeleteCount.Load())
	newMetrics.KeyCount.Store(s.storageMetrics.KeyCount.Load())
	newMetrics.ByteSize.Store(s.storageMetrics.ByteSize.Load())

	return &InmemStore{
		buckets:        bucks,
		storageMetrics: newMetrics,
	}
}
