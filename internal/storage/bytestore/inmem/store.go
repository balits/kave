package inmem

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	"github.com/google/btree"
)

type InmemStore struct {
	rwlock  sync.RWMutex
	buckets map[storage.Bucket]*btree.BTree
	sz      atomic.Int64
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
		rwlock:  sync.RWMutex{},
		buckets: buckets,
	}
}

// ========= storage.KVStore impl =========

func (s *InmemStore) Get(bucket storage.Bucket, key []byte) (value []byte, err error) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.unsafeGet(bucket, key)
}

func (s *InmemStore) Put(bucket storage.Bucket, key, value []byte) (old []byte, err error) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	return s.unsafePut(bucket, key, value)
}

func (s *InmemStore) Delete(bucket storage.Bucket, key []byte) (value []byte, err error) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	return s.unsafeDelete(bucket, key)
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

func (s *InmemStore) SizeBytes() int64 {
	return s.sz.Load()
}

func (s *InmemStore) Defragment() error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	//TODO build new tree after iterating
	return nil
}

func (s *InmemStore) Close() error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	for _, tree := range s.buckets {
		tree.Clear(false)
	}
	return nil
}

func (s *InmemStore) Ping() error {
	return nil
}

// ========= kv internals =========
// these functions know nothing about transactions or locks
// they purely perform their logic and do some smetrics
// which is preferred in batches, since they should only lock/unlock once for the whole batch

func (s *InmemStore) unsafeGet(bucket storage.Bucket, key []byte) ([]byte, error) {
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

func (s *InmemStore) unsafePut(bucket storage.Bucket, key, value []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, storage.ErrEmptyKey
	}

	tree, ok := s.buckets[bucket]
	if !ok {
		return nil, storage.ErrBucketNotFound
	}

	old := tree.ReplaceOrInsert(KVBtreeItem{key, value})
	if old != nil {
		return old.(KVBtreeItem).Value, nil
	}
	return nil, nil
}

func (s *InmemStore) unsafeDelete(bucket storage.Bucket, key []byte) ([]byte, error) {
	tree, ok := s.buckets[bucket]
	if !ok {
		return nil, storage.ErrBucketNotFound
	}

	old := tree.Delete(KVBtreeItem{Key: key})
	if old != nil {
		return old.(KVBtreeItem).Value, nil
	}
	return nil, nil
}
