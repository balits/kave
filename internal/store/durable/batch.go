package durable

import (
	"github.com/balits/kave/internal/store"
	bolt "go.etcd.io/bbolt"
)

type batch struct {
	bc     store.BatchCollector
	tx     *bolt.Tx
	closed bool
}

func newBatch(tx *bolt.Tx) store.Batch {
	return &batch{
		tx: tx,
		bc: store.NewBatchCollector(),
	}
}

func (b *batch) Set(key, value []byte) error {
	if b.closed {
		return store.ErrBatchClosed
	}
	b.bc.RecordSet(key, value)
	return nil
}

func (b *batch) Delete(key []byte) error {
	if b.closed {
		return store.ErrBatchClosed
	}
	b.bc.RecordDelete(key)
	return nil
}

func (b *batch) Commit() error {
	if b.closed {
		return store.ErrBatchClosed
	}

	bucket := b.tx.Bucket([]byte(store.BucketKV))

	if bucket == nil {
		return store.ErrBucketNotFound
	}

	for key := range b.bc.Deletes() {
		keyb := []byte(key)
		if err := bucket.Delete(keyb); err != nil {
			return err
		}
	}

	for k, v := range b.bc.Writes() {
		if err := bucket.Put([]byte(k), v); err != nil {
			return err
		}
	}

	b.closed = true
	return b.tx.Commit()
}

func (b *batch) Abort() error {
	if b.closed {
		return store.ErrBatchClosed
	}

	b.closed = true
	b.bc.Reset()
	return b.tx.Rollback()
}
