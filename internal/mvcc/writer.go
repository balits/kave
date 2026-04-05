package mvcc

import (
	"fmt"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types"
)

type Writer interface {
	// writer should implement normal read operations too, since we have the exclusive writelock
	Reader
	// returns the writers start revision
	Revision() kv.Revision

	Put(key, value []byte, leaseID int64) (rev kv.Revision, err error)
	DeleteRange(key, end []byte) (count int64, rev kv.Revision, err error)
	DeleteKey(key []byte) (count int64, rev kv.Revision, err error)

	// TODO: support rev.sub++ for txp ops
	// TxnMode() bool

	// End commits the transaction and releases locks.
	// It updates the store's current revision and raft metadata, so it should be called after all changes are made.
	// It should be called if the caller decides to commit the transaction.
	End()

	// Abort discards all changes and releases locks. It should be called if the caller decides not to commit the transaction.
	Abort()
	Changes() []types.KvEntry
}

type writer struct {
	store    *KvStore
	writeTx  backend.WriteTx
	startRev kv.Revision
	changes  []types.KvEntry

	startTime time.Time
}

func newWriter(
	store *KvStore,
	writeTx backend.WriteTx,
	startRev kv.Revision,
) *writer {
	return &writer{
		store:     store,
		writeTx:   writeTx,
		startRev:  startRev,
		startTime: time.Now(),
	}
}

func (w *writer) Revision() kv.Revision    { return w.startRev }
func (w *writer) Changes() []types.KvEntry { return w.changes }

// delegates

func (w *writer) Get(key []byte, rev int64) *types.KvEntry {
	entries, _, _, err := w.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return &entries[0]
}

func (w *writer) Range(key, end []byte, rev int64, limit int64) (entries []types.KvEntry, count int, currentRev int64, err error) {
	start := time.Now()
	w.store.revMu.RLock()
	curRevMain := w.store.currentRev.Main
	compactRev := w.store.compactedMainRev
	w.store.revMu.RUnlock()

	w.store.metrics.ReadsTotal.Inc()
	defer func() { w.store.metrics.ReadDurationSec.Observe(time.Since(start).Seconds()) }()

	if rev > curRevMain {
		return nil, 0, curRevMain, fmt.Errorf("future revision requested")
	}
	if rev <= 0 {
		rev = curRevMain
	}
	if rev < compactRev {
		return nil, 0, 0, kv.ErrCompacted
	}

	revpairs, total := w.store.kvIndex.Revisions(key, end, rev, int(limit))
	if len(revpairs) == 0 {
		return nil, total, curRevMain, nil
	}

	lim := int(limit)
	if lim <= 0 || lim > len(revpairs) {
		lim = len(revpairs)
	}

	entries = make([]types.KvEntry, 0, lim)
	revBytes := kv.NewRevBytes()
	for _, rp := range revpairs[:lim] {
		revBytes = kv.EncodeRevisionAsBucketKey(rp, revBytes)
		entryBytes, err := w.writeTx.UnsafeGet(schema.BucketKV, revBytes)
		if err != nil || entryBytes == nil {
			w.store.logger.Error("range: revision not found in backend", "main", rp.Main, "sub", rp.Sub)
			continue
		}
		entry, err := types.DecodeKvEntry(entryBytes)
		if err != nil {
			w.store.logger.Error("range: failed to unmarshal entry", "err", err)
			continue
		}
		entries = append(entries, entry)
	}

	return entries, total, curRevMain, nil
}

func (w *writer) Put(key []byte, value []byte, leaseID int64) (kv.Revision, error) {
	if err := w.put(key, value, leaseID); err != nil {
		w.store.logger.Error("mvcc.Writer.Put() failed", "error", err)
		w.store.metrics.PutErrorsTotal.Inc()
		return kv.Revision{}, err
	}
	return kv.Revision{Main: w.startRev.Main + 1}, nil
}

func (w *writer) put(key, value []byte, leaseID int64) error {
	nextRev := w.startRev.Main + 1
	createRev := nextRev // create revision defaults to this nextRev

	// Check if key already exists — reuse its create revision
	created, _, version, err := w.store.kvIndex.Get(key, nextRev)
	if err == nil {
		createRev = created.Main
	}

	idxRev := kv.Revision{Main: nextRev, Sub: int64(len(w.changes))}
	idxRevBytes := kv.NewRevBytes()
	idxRevBytes = kv.EncodeRevisionAsBucketKey(idxRev, idxRevBytes)

	entry := types.KvEntry{
		Key:       key,
		Value:     value,
		CreateRev: createRev,
		ModRev:    nextRev,
		Version:   version + 1,
		LeaseID:   leaseID,
	}

	d, err := types.EncodeKvEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode entry: %v", err)
	}

	if err := w.writeTx.UnsafePut(schema.BucketKV, idxRevBytes, d); err != nil {
		return fmt.Errorf("failed to put entry: %v", err)
	}
	if err := w.store.kvIndex.Put(key, idxRev); err != nil {
		return fmt.Errorf("failed to update index: %v", err)
	}

	w.changes = append(w.changes, entry)
	return nil
}

func (w *writer) DeleteRange(key []byte, end []byte) (count int64, rev kv.Revision, err error) {
	count, err = w.deleteRange(key, end)
	if err != nil {
		w.store.logger.Error("mvcc.Writer.DeleteRange() failed", "error", err)
		w.store.metrics.DeleteErrorsTotal.Inc()
		return 0, kv.Revision{}, err
	}
	if count != 0 || len(w.changes) > 0 {
		return count, kv.Revision{Main: w.startRev.Main + 1}, nil
	}
	return 0, w.startRev, nil
}

func (w *writer) deleteRange(key []byte, end []byte) (count int64, err error) {
	startRev := w.startRev.Main
	if len(w.changes) > 0 {
		startRev++
	}

	keys, revs := w.store.kvIndex.Range(key, end, startRev)
	if revs == nil {
		// this can mean either that there are no keys in the range, or that the range is compacted
		// we can distinguish these cases by checking if the startRev is less than the compacted rev
		// for now, leave it as a no-op
		return 0, nil
		//return 0, errors.New("mvcc.Writer.DeleteRange() failed: error during index.Range(): no revisions returned")
	}

	count = int64(len(keys))
	if count == 0 {
		return 0, nil
	}
	for _, k := range keys {
		if err := w.deleteKey(k); err != nil {
			return 0, fmt.Errorf("mvcc.Writer.DeleteRange() failed: error during deleteKey(): %v", err)
		}
	}
	return
}

func (w *writer) DeleteKey(key []byte) (count int64, rev kv.Revision, err error) {
	return w.DeleteRange(key, nil)
}

func (w *writer) deleteKey(key []byte) error {
	bk := kv.NewKvBucketKey(w.startRev.Main+1, int64(len(w.changes)), true)
	bkBytes := kv.NewRevBytes()
	bkBytes = kv.EncodeKvBucketKey(bk, bkBytes)

	entry := types.KvEntry{Key: key}
	entryBytes, err := types.EncodeKvEntry(entry)
	if err != nil {
		return fmt.Errorf("writer.deleteKey(): failed to encode entry: %v", err)
	}

	// update history
	err = w.writeTx.UnsafePut(schema.BucketKV, bkBytes, entryBytes)
	if err != nil {
		return fmt.Errorf("writer.deleteKey(): failed to put tombstone entry: %v", err)
	}

	// update index
	err = w.store.kvIndex.Tombstone(key, bk.Revision)
	if err != nil {
		return fmt.Errorf("failed to tombstone key: %s, error: %v", string(key), err)
	}

	w.changes = append(w.changes, entry)
	return nil
}

func (w *writer) Abort() {
	w.changes = nil
	w.writeTx.Abort()
	w.writeTx.Unlock()       // release db lock
	w.store.rwlock.RUnlock() // release store lock
}

func (w *writer) End() {
	if len(w.changes) != 0 {
		w.store.revMu.Lock()
		w.store.currentRev = kv.Revision{Main: w.store.currentRev.Main + 1}
		w.writeTx.UnsafePut(schema.BucketMeta, schema.KeyCurrentRevision, types.EncodeUint64(uint64(w.store.currentRev.Main)))
	}

	if w.store.applyIndex > 0 {
		w.writeTx.UnsafePut(schema.BucketMeta, schema.KeyRaftApplyIndex, types.EncodeUint64(w.store.applyIndex))
		w.writeTx.UnsafePut(schema.BucketMeta, schema.KeyRaftTerm, types.EncodeUint64(w.store.raftTerm))
	}

	info, err := w.writeTx.Commit()
	if err != nil {
		w.store.logger.Error("failed to commit write tx", "error", err)
	} else {
		w.store.metrics.TxnsTotal.Add(1) // TODO: count failed txns?
		w.store.metrics.TxnDurationSec.Observe(time.Since(w.startTime).Seconds())
		w.store.metrics.PutsTotal.Add(float64(info.NewKeys))
		w.store.metrics.DeletesTotal.Add(float64(info.DeletedKeys))
		w.store.metrics.KeyCount.Add(float64(info.NewKeys - info.DeletedKeys))
	}
	w.writeTx.Unlock() // release db lock

	if len(w.changes) != 0 {
		w.store.revMu.Unlock()
	}
	w.store.rwlock.RUnlock() // release store lock
}
