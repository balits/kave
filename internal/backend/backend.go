package backend

import (
	"io"
	"sync"

	"github.com/balits/kave/internal/storage"
)

// Backend builds transactions on top of storage providing:
// - atomic multi-key, multi-bucket writes
// - consistent read snapshots
// - batching
type Backend interface {
	ReadTx() ReadTx
	WriteTx() WriteTx

	Snapshot(w io.Writer) error
	Restore(io.Reader) error

	// TODO:
	// SizeHint() int64  // hint of the size of the db
	// SizeInUse() int64 // size of the db in use

	// TOOD
	//Defragment() error

	ForceCommit() error
	Commit() error
	Close() error
}

type backend struct {
	rwlock sync.RWMutex
	store  storage.Storage
	batch  storage.Batch
}

func NewBackend(store storage.Storage) Backend {
	return &backend{
		store: store,
		batch: nil,
	}
}

func (b *backend) ReadTx() ReadTx {
	return &readtx{b: b}
}

func (b *backend) WriteTx() WriteTx {
	return &writetx{
		b:      b,
		readtx: &readtx{b},
	}
}

func (b *backend) Snapshot(w io.Writer) error {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	_, err := b.store.WriteTo(w)
	return err
}

func (b *backend) Restore(r io.Reader) error {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()
	_, err := b.store.ReadFrom(r)
	return err
}

func (b *backend) ForceCommit() error {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()

	// If there’s a pending batch, commit it
	if b.batch != nil {
		err := b.batch.Commit()
		b.batch = nil
		return err
	}

	return nil
}

func (b *backend) Commit() error {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()

	if b.batch == nil {
		return nil
	}
	err := b.batch.Commit()
	b.batch = nil
	return err
}

func (b *backend) Defrag() error {
	return b.store.Defrag()
}

func (b *backend) Close() error {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()

	if b.batch != nil {
		b.batch.Abort()
		b.batch = nil
	}

	var err error
	if b.store != nil {
		err = b.store.Close()
		b.store = nil
	}
	return err
}
