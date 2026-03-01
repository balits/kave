package backend

import (
	"fmt"

	"github.com/balits/kave/internal/storage"
)

type WriteTx interface {
	ReadTx

	Lock()
	Unlock()

	UnsafePut(bucket storage.Bucket, key, value []byte) error
	UnsafeDelete(bucket storage.Bucket, key []byte) error

	Commit() error
	Abort()
}

// TODO: impl batching to reduce fsyncs in the future
type writetx struct {
	*readtx
	b *backend
}

func (w *writetx) Lock() {
	w.b.rwlock.Lock()
	batch, err := w.b.store.NewBatch()
	if err != nil {
		// this can only happen if:
		// 1) db not opened
		// 2) db closed
		// 3) db.data == nil, wrongly mapped
		// these should never happend, and if it does, we are in a very bad state, so we just panic
		panic(fmt.Errorf("WriteTx locking failed: failed to create new Batch: %v", err))
	}
	w.b.batch = batch
}
func (w *writetx) Unlock() { w.b.rwlock.Unlock() }

// NOTE: caller needs to hold the lock
func (w *writetx) UnsafePut(bucket storage.Bucket, key, value []byte) error {
	return w.b.batch.Put(bucket, key, value)
}

// NOTE: caller needs to hold the lock
func (w *writetx) UnsafeDelete(bucket storage.Bucket, key []byte) error {
	return w.b.batch.Delete(bucket, key)
}

func (w *writetx) Commit() error {
	defer func() {
		w.b.batch = nil
	}()
	return w.b.batch.Commit()
}

func (w *writetx) Abort() {
	defer func() {
		w.b.batch = nil
	}()
	w.b.batch.Abort()
}
