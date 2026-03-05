package durable

import (
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	bolt "go.etcd.io/bbolt"
)

type durableBatch struct {
	tx        *bolt.Tx
	wc        bytestore.WriteCollector
	bucketMap map[storage.Bucket][]byte
	closed    bool
}

func newBatch(tx *bolt.Tx, bucketMap map[storage.Bucket][]byte) bytestore.Batch {
	return &durableBatch{
		tx:        tx,
		wc:        bytestore.NewWriteCollector(),
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

func (b *durableBatch) Commit() error {
	if b.closed {
		return storage.ErrBatchClosed
	}

	for bucketName, keys := range b.wc.Deletes() {
		bucketBytes, ok := b.bucketMap[bucketName]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := b.tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		for key := range keys {
			if len(key) == 0 {
				return storage.ErrEmptyKey
			}
			keyb := []byte(key)
			if err := bucket.Delete(keyb); err != nil {
				return err
			}
		}
	}

	for bucketName, keys := range b.wc.Puts() {
		bucketBytes, ok := b.bucketMap[bucketName]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := b.tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		for k, v := range keys {
			if len(k) == 0 {
				return storage.ErrEmptyKey
			}
			if err := bucket.Put([]byte(k), v); err != nil {
				return err
			}
		}
	}

	b.closed = true
	return b.tx.Commit()
}

func (b *durableBatch) Abort() {
	if b.closed {
		return
	}

	b.closed = true
	b.wc.Reset()
	b.tx.Rollback()
}
