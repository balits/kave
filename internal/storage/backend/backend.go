package backend

import (
	"fmt"
	"io"
	"sync"

	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	"github.com/balits/kave/internal/storage/bytestore/durable"
	"github.com/balits/kave/internal/storage/bytestore/inmem"
	"github.com/prometheus/client_golang/prometheus"
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

	// size of the db in use
	SizeBytes() int64

	// TOOD
	//Defragment() error

	//ForceCommit() error
	// Commit() error // commit should be the responsibility of the write transaction
	Close() error

	Ping() error
}

type backend struct {
	rwlock  sync.RWMutex
	store   bytestore.ByteStore
	batch   bytestore.Batch
	metrics *metrics.BackendMetrics
	obs     BackendObserver
}

func NewBackend(reg prometheus.Registerer, opts storage.StorageOptions) Backend {
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

	b := &backend{
		store: s,
		batch: nil,
	}
	sizeBytesFn := func() float64 {
		return float64(b.SizeBytes())
	}
	b.metrics = metrics.NewBackendMetrics(reg, sizeBytesFn)
	b.obs = &observer{metrics: b.metrics}

	return b
}

func (b *backend) SizeBytes() int64 {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	return b.store.SizeBytes()
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

func (b *backend) Commit() (storage.CommitInfo, error) {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()

	if b.batch == nil {
		return storage.CommitInfo{}, fmt.Errorf("attempting to commit a nil batch")
	}
	b.batch = nil
	return b.batch.Commit()
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

func (b *backend) Ping() error {
	return b.store.Ping()
}
