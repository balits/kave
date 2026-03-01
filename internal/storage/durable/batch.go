package durable

import (
	"github.com/balits/kave/internal/storage"
	bolt "go.etcd.io/bbolt"
)

type durableBatch struct {
	tx        *bolt.Tx
	wc        storage.WriteCollector
	bucketMap map[storage.Bucket][]byte
	closed    bool
}

func newBatch(tx *bolt.Tx, bucketMap map[storage.Bucket][]byte) storage.Batch {
	return &durableBatch{
		tx: tx,
		wc: storage.NewWriteCollector(),
		bucketMap: bucketMap,
	}
}

func (b *durableBatch) Put(bucket storage.Bucket, key, value []byte) error {
	if b.closed {
		return storage.ErrBatchClosed
	}
	b.wc.RecordPut(bucket, key, value)
	return nil
}

func (b *durableBatch) Delete(bucket storage.Bucket, key []byte) error {
	if b.closed {
		return storage.ErrBatchClosed
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
			if err := bucket.Put([]byte(k), v); err != nil {
				return err
			}
		}
	}

	b.closed = true
	return b.tx.Commit()
}

func (b *durableBatch) Abort() error {
	if b.closed {
		return storage.ErrBatchClosed
	}

	b.closed = true
	b.wc.Reset()
	return b.tx.Rollback()
}
