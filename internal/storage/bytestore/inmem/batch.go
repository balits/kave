package inmem

import (
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
)

type inmemBatch struct {
	inner  *InmemStore
	closed bool
	wc     bytestore.WriteCollector
}

func newBatch(s *InmemStore) bytestore.Batch {
	return &inmemBatch{
		inner: s,
		wc:    bytestore.NewWriteCollector(),
	}
}

func (b *inmemBatch) Put(bucket storage.Bucket, key, value []byte) error {
	if b.closed {
		return storage.ErrBatchClosed
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}
	b.wc.RecordPut(bucket, key, value)
	return nil
}

func (b *inmemBatch) Delete(bucket storage.Bucket, key []byte) error {
	if b.closed {
		return storage.ErrBatchClosed
	}
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}
	b.wc.RecordDelete(bucket, key)
	return nil
}

func (b *inmemBatch) Commit() error {
	if b.closed {
		return storage.ErrBatchClosed
	}

	defer func() {
		b.closed = true
	}()

	b.inner.rwlock.Lock()
	defer b.inner.rwlock.Unlock()

	for bucket, keys := range b.wc.Deletes() {
		for key := range keys {
			_, err := b.inner.doDelete(bucket, []byte(key))
			if err != nil {
				return err
			}
		}
	}

	for bucket, keys := range b.wc.Puts() {
		for key, value := range keys {
			err := b.inner.doSet(bucket, []byte(key), value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *inmemBatch) Abort() {
	if b.closed {
		return
	}
	b.closed = true
	b.wc.Reset()
}
