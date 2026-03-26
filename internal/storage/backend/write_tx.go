package backend

import (
	"fmt"
	"sync"
	"time"

	"github.com/balits/kave/internal/storage"
)

type WriteTx interface {
	ReadTx
	sync.Locker

	// NOTE: caller needs to hold the lock
	UnsafePut(bucket storage.Bucket, key, value []byte) error

	// NOTE: caller needs to hold the lock
	UnsafeDelete(bucket storage.Bucket, key []byte) error

	// Commits applies the previous changes to the database,
	// returning the information about the committed batch.
	// If an error happened during Commit, the caller should
	// [Abort] the transaction.
	Commit() (storage.CommitInfo, error)

	// Abort discards all previous changes made in the transaction.
	//
	// NOTE: this does not release the backend mutex.
	// the caller should call Unlock after Abort manually.
	Abort()
}

// TODO: impl batching to reduce fsyncs in the future
type writetx struct {
	*readtx
	b       *backend
	opCount int
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

func (w *writetx) UnsafePut(bucket storage.Bucket, key, value []byte) error {
	w.opCount++
	return w.b.batch.Put(bucket, key, value)
}

func (w *writetx) UnsafeDelete(bucket storage.Bucket, key []byte) error {
	w.opCount++
	return w.b.batch.Delete(bucket, key)
}

func (w *writetx) Commit() (storage.CommitInfo, error) {
	start := time.Now()
	defer func() {
		w.b.batch = nil
		w.opCount = 0
	}()
	info, err := w.b.batch.Commit()
	if err == nil && w.b.obs != nil {
		w.b.obs.ObserveCommit(time.Since(start), w.opCount)
	}
	return info, err
}

func (w *writetx) Abort() {
	defer func() {
		w.b.batch = nil
	}()
	w.b.batch.Abort()
}
