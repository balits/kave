package mvcc

import (
	"fmt"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage/backend"
)

type Writer interface {
	// writer should implement normal read operations too, since we have the exclusive writelock
	Reader
	// returns the writers start revision
	kv.RevisionGetter

	Put(key, value []byte) kv.Revision
	DeleteRange(key, end []byte) (count int64, rev kv.Revision)
	DeleteKey(key []byte) (count int64, rev kv.Revision)

	// End commits the transaction and releases locks.
	// It updates the store's current revision and raft metadata, so it should be called after all changes are made.
	// It should be called if the caller decides to commit the transaction.
	End()

	// Abort discards all changes and releases locks. It should be called if the caller decides not to commit the transaction.
	Abort()
	Changes() []kv.Entry
}

type writer struct {
	store    *KVStore
	writeTx  backend.WriteTx
	startRev kv.Revision
	changes  []kv.Entry
}

func (w *writer) Revision() kv.Revision { return w.startRev }
func (w *writer) Changes() []kv.Entry   { return w.changes }

// delegates

func (w *writer) Get(key []byte, rev int64) *kv.Entry {
	entries, _, _, err := w.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return &entries[0]
}

func (w *writer) Range(key, end []byte, rev int64, limit int64) (entries []kv.Entry, count int, currentRev int64, err error) {
	w.store.revMu.RLock()
	curRevMain := w.store.currentRev.Main
	compactRev := w.store.compactedMainRev
	w.store.revMu.RUnlock()

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

	entries = make([]kv.Entry, 0, lim)
	revBytes := kv.NewRevBytes()
	for _, rp := range revpairs[:lim] {
		revBytes = kv.RevToBytes(rp, revBytes)
		entryBytes, err := w.writeTx.UnsafeGet(kv.BucketMain, revBytes)
		if err != nil || entryBytes == nil {
			w.store.logger.Error("range: revision not found in backend", "main", rp.Main, "sub", rp.Sub)
			continue
		}
		entry, err := kv.DecodeEntry(entryBytes)
		if err != nil {
			w.store.logger.Error("range: failed to unmarshal entry", "err", err)
			continue
		}
		entries = append(entries, entry)
	}

	return entries, total, curRevMain, nil
}

func (w *writer) Put(key []byte, value []byte) kv.Revision {
	w.put(key, value)
	return kv.Revision{Main: w.startRev.Main + 1}
}

// TODO: panics
func (w *writer) put(key, value []byte) {
	nextRev := w.startRev.Main + 1
	createRev := nextRev // create revision defaults to this nextRev

	// Check if key already exists — reuse its create revision
	created, _, version, err := w.store.kvIndex.Get(key, nextRev)
	if err == nil {
		createRev = created.Main
	}

	idxRev := kv.Revision{Main: nextRev, Sub: int64(len(w.changes))}
	idxRevBytes := kv.NewRevBytes()
	idxRevBytes = kv.RevToBytes(idxRev, idxRevBytes)

	entry := kv.Entry{
		Key:       key,
		Value:     value,
		CreateRev: createRev,
		ModRev:    nextRev,
		Version:   version + 1,
	}

	d, err := kv.EncodeEntry(entry)
	if err != nil {
		panic(fmt.Sprintf("failed to encode entry: %v", err))
	}

	if err := w.writeTx.UnsafePut(kv.BucketMain, idxRevBytes, d); err != nil {
		panic(fmt.Sprintf("failed to put entry: %v", err))
	}
	if err := w.store.kvIndex.Put(key, idxRev); err != nil {
		panic(fmt.Sprintf("failed to update index: %v", err))
	}

	w.changes = append(w.changes, entry)
}

func (w *writer) DeleteRange(key []byte, end []byte) (n int64, rev kv.Revision) {
	if count := w.deleteRange(key, end); count != 0 || len(w.changes) > 0 {
		return count, kv.Revision{Main: w.startRev.Main + 1}
	}
	return 0, w.startRev
}

func (w *writer) deleteRange(key []byte, end []byte) (count int64) {
	startRev := w.startRev.Main
	if len(w.changes) > 0 {
		startRev++
	}

	keys, err := w.store.kvIndex.Range(key, end, startRev)
	if err != nil {
		w.store.logger.Error("mvcc.Writer.DeleteRange() failed: error during index.Range()", "error", err)
	}

	count = int64(len(keys))
	if count == 0 {
		return 0
	}
	for _, k := range keys {
		w.deleteKey(k)
	}
	return
}

func (w *writer) DeleteKey(key []byte) (count int64, rev kv.Revision) {
	return w.DeleteRange(key, nil)
}

func (w *writer) deleteKey(key []byte) error {
	bk := kv.NewBucketKey(w.store.currentRev.Main+1, int64(len(w.changes)), true)
	bkBytes := kv.NewRevBytes()
	bkBytes = kv.BucketKeyToBytes(bk, bkBytes)

	entry := kv.Entry{Key: key}
	entryBytes, err := kv.EncodeEntry(entry)
	if err != nil {
		panic(fmt.Sprintf("failed to encode entry: %v", err))
	}

	// update history
	err = w.writeTx.UnsafePut(kv.BucketMain, bkBytes, entryBytes)
	if err != nil {
		panic(fmt.Sprintf("failed to put tombstone entry: %v", err))
	}

	// update index
	err = w.store.kvIndex.Tombstone(key, bk.Revision)
	if err != nil {
		w.store.logger.Error("failed to tombstone key", "key", string(key), "error", err)
		return err // TODO PANIC?
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
		w.writeTx.UnsafePut(kv.BucketMeta, kv.MetaKeyCurrentRevision, kv.EncodeUint64(uint64(w.store.currentRev.Main)))
	}

	if w.store.raftIndex > 0 {
		w.writeTx.UnsafePut(kv.BucketMeta, kv.MetaKeyRaftApplyIndex, kv.EncodeUint64(w.store.raftIndex))
		w.writeTx.UnsafePut(kv.BucketMeta, kv.MetaKeyRaftTerm, kv.EncodeUint64(w.store.raftTerm))
	}

	if err := w.writeTx.Commit(); err != nil {
		w.store.logger.Error("failed to commit write tx", "error", err)
	}
	w.writeTx.Unlock() // release db lock

	if len(w.changes) != 0 {
		w.store.revMu.Unlock()
	}
	w.store.rwlock.RUnlock() // release store lock
}
