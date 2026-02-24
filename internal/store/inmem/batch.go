package inmem

import (
	"github.com/balits/kave/internal/store"
)

type batch struct {
	inner  *Store
	closed bool
	bc     store.BatchCollector
}

func newBatch(s *Store) store.Batch {
	return &batch{
		inner: s,
		bc:    store.NewBatchCollector(),
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

	defer func() {
		b.closed = true
	}()

	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()

	for key := range b.bc.Deletes() {
		keyb := []byte(key)
		_, err := b.inner.doDelete(store.BucketKV, keyb)
		if err != nil {
			return err
		}
	}

	for k, v := range b.bc.Writes() {
		keyb := []byte(k)
		err := b.inner.doSet(store.BucketKV, keyb, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *batch) Abort() error {
	b.closed = true
	b.bc.Reset()
	return nil
}
