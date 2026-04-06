package durable

import (
	"sync/atomic"

	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	bolt "go.etcd.io/bbolt"
)

type durableBatch struct {
	tx        *bolt.Tx
	wc        bytestore.WriteCollector
	sz        *atomic.Int64
	bucketMap map[storage.Bucket][]byte
	closed    bool
}

func newBatch(tx *bolt.Tx, bucketMap map[storage.Bucket][]byte, sz *atomic.Int64) bytestore.Batch {
	return &durableBatch{
		tx:        tx,
		wc:        bytestore.NewWriteCollector(),
		sz:        sz,
		bucketMap: bucketMap,
	}
}

func (b *durableBatch) Put(bucket storage.Bucket, key, value []byte) error {
	if b.closed {
		return storage.ErrBatchClosed
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}
	b.wc.RecordPut(bucket, key, value)
	return nil
}

func (b *durableBatch) Delete(bucket storage.Bucket, key []byte) error {
	if b.closed {
		return storage.ErrBatchClosed
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}
	b.wc.RecordDelete(bucket, key)
	return nil
}

func (b *durableBatch) Commit() (info storage.CommitInfo, err error) {
	if b.closed {
		return info, storage.ErrBatchClosed
	}

	totalDelta := int64(0)
	defer func() {
		b.closed = true
		// accumulate potential size difference,
		// and only update stores sizes if there was no error
		if err == nil {
			b.sz.Add(totalDelta)
		}
	}()

	for bucketName, keys := range b.wc.Deletes() {
		bucketBytes, ok := b.bucketMap[bucketName]
		if !ok {
			return info, storage.ErrBucketNotFound
		}
		bucket := b.tx.Bucket(bucketBytes)
		if bucket == nil {
			return info, storage.ErrBucketNotFound
		}

		for key := range keys {
			if len(key) == 0 {
				return info, storage.ErrEmptyKey
			}
			keyb := []byte(key)
			old := bucket.Get(keyb)
			if old != nil {
				totalDelta += int64(-len(old))
				info.DeletedKeys++
			}
			if err := bucket.Delete(keyb); err != nil {
				return info, err
			}
		}
	}

	for bucketName, keys := range b.wc.Puts() {
		bucketBytes, ok := b.bucketMap[bucketName]
		if !ok {
			return info, storage.ErrBucketNotFound
		}
		bucket := b.tx.Bucket(bucketBytes)
		if bucket == nil {
			return info, storage.ErrBucketNotFound
		}

		for k, v := range keys {
			if len(k) == 0 {
				return info, storage.ErrEmptyKey
			}
			old := bucket.Get([]byte(k))
			if old != nil {
				totalDelta += int64(len(old) - len(v))
			} else {
				totalDelta += int64(len(v))
			}
			if err := bucket.Put([]byte(k), v); err != nil {
				return info, err
			} else if old == nil {
				info.NewKeys++
			}
		}
	}

	return info, b.tx.Commit()
}

func (b *durableBatch) Abort() {
	if b.closed {
		return
	}

	b.closed = true
	b.wc.Reset()
	_ = b.tx.Rollback() // TODO: maybe put a logger on the store?
}
