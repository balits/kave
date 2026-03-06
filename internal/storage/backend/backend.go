package backend

import (
	"fmt"
	"io"
	"sync"

	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	"github.com/balits/kave/internal/storage/bytestore/durable"
	"github.com/balits/kave/internal/storage/bytestore/inmem"
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
	store  bytestore.ByteStore
	batch  bytestore.Batch
}

func NewBackend(opts storage.StorageOptions) Backend {
	var s bytestore.ByteStore
	switch opts.Kind {
	case storage.StorageKindBoltdb:
		var err error
		s, err = durable.NewStore(opts)
		if err != nil {
			panic(fmt.Sprintf("failed to create bytestore: %v", err))
		}
	case storage.StorageKindInMemory:
		s = inmem.NewStore(opts)
	}

	return &backend{
		store: s,
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
	return b.store.Defragment()
}

func (b *backend) Close() error {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()

	if b.batch != nil {
		b.batch.Abort()
		b.batch = nil
	}

	return b.store.Close()
}
