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

func (b *inmemBatch) Commit() (info storage.CommitInfo, err error) {
	if b.closed {
		err = storage.ErrBatchClosed
		return
	}

	totalDelta := int64(0)

	defer func() {
		b.closed = true
		// accumulate potential size difference,
		// and only update stores sizes if there was no error
		if err == nil {
			b.inner.sz.Add(totalDelta)
		}
	}()

	b.inner.rwlock.Lock()
	defer b.inner.rwlock.Unlock()

	for bucket, keys := range b.wc.Deletes() {
		for key := range keys {
			var old []byte
			old, err = b.inner.unsafeDelete(bucket, []byte(key))
			if err != nil {
				return
			}
			if old != nil {
				totalDelta += int64(-len(old))
				info.DeletedKeys++
			}
		}
	}

	for bucket, keys := range b.wc.Puts() {
		for key, value := range keys {
			var old []byte
			old, err = b.inner.unsafePut(bucket, []byte(key), value)
			if err != nil {
				return
			}
			if old != nil {
				totalDelta += int64(len(old) - len(value))
			} else {
				totalDelta += int64(len(value))
				info.NewKeys++
			}
		}
	}

	return
}

func (b *inmemBatch) Abort() {
	if b.closed {
		return
	}
	b.closed = true
	b.wc.Reset()
}
