package mvcc

import (
	"fmt"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
)

type Writer interface {
	// writer should implement normal read operations too, since we have the exclusive writelock,
	// but the write transaction does not end at reading, so callers should still call w.End() or w.Abort().
	Reader

	// StartRev returns the writers start revision,
	// and it gets updated when the writer commits a transaction.

	// Put writes a key-value pair with the given lease ID.
	// The caller should call End() to commit the transaction and update the store's current revision.
	Put(key, value []byte, leaseID int64) error

	// Delete deletes a [key, end) range if end is provided, or a single key if end is nil.
	// The caller should call End() to commit the transaction and update the store's current revision.
	DeleteRange(key, end []byte) error

	// DeleteKey deletes a single key. It's a convenience method that calls DeleteRange with end = nil.
	// The caller should call End() to commit the transaction and update the store's current revision.
	DeleteKey(key []byte) error

	// End commits the transaction and releases locks, returning any errors that mightve happened.
	// It updates the store's current revision and raft metadata, so it should be called after all changes are made.
	// It should be called if the caller decides to commit the transaction.
	End() error

	// Abort discards all changes and releases locks. It should be called if the caller decides not to commit the transaction.
	Abort()

	// StartRevision returns the revision at the beginning of the writer,
	// before any writes were applied.
	StartRevision() kv.Revision

	// UnsafeExpectedChanges returns all the changes made up until this point in time,
	// and the expected end revision which would be the new global revision after succesfull call to w.End()
	//
	// NOTE: its only safe to rely on the expected end revision after calling w.End() without it producing an error
	UnsafeExpectedChanges() (unsafeEndRev int64, unsafeChanges []*kv.Entry)
}

type writer struct {
	store    *KvStore
	writeTx  backend.WriteTx
	startRev kv.Revision
	changes  []*kv.Entry

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

func (w *writer) Revisions() (current kv.Revision, compacted int64) {
	return w.store.Revisions()
}

func (w *writer) StartRevision() kv.Revision { return w.startRev }

func (w *writer) UnsafeExpectedChanges() (int64, []*kv.Entry) {
	return w.startRev.Main + 1, w.changes
}

func (w *writer) Get(key []byte, rev int64) *kv.Entry {
	entries, _, _, err := w.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return entries[0]
}

func (w *writer) Range(key, end []byte, targetRev int64, limit int64) (entries []*kv.Entry, count int, currentRev int64, err error) {
	start := time.Now()
	w.store.metrics.ReadsTotal.Inc()
	defer func() { w.store.metrics.ReadDurationSec.Observe(time.Since(start).Seconds()) }()

	currRev, compactedRev := w.store.Revisions()
	return doRange(w.store.logger, w.store.kvIndex, w.writeTx, currRev.Main, compactedRev, targetRev, key, end, limit)
}

// As this is an 'internal', non public facing API, we dont need to track metrics here
// its only used for the watch implementation, for which we will track different group of metrics
func (w *writer) RevisionRange(startRev, endRev int64, limit int64) (entries []*kv.Entry, err error) {
	currRev, compactedRev := w.store.Revisions()
	return doRevisionRange(w.store.logger, w.writeTx, startRev, endRev, currRev.Main, compactedRev, limit)
}

func (w *writer) Put(key []byte, value []byte, leaseID int64) error {
	if err := w.put(key, value, leaseID); err != nil {
		w.store.logger.Error("mvcc.Writer.Put() failed", "error", err)
		w.store.metrics.PutErrorsTotal.Inc()
		return fmt.Errorf("mvcc.Writer.Put() failed: %w", err)
	}
	return nil
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

	entry := &kv.Entry{
		Key:       key,
		Value:     value,
		CreateRev: createRev,
		ModRev:    nextRev,
		Version:   version + 1,
		LeaseID:   leaseID,
	}

	d, err := kv.EncodeKvEntry(entry)
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

func (w *writer) DeleteRange(key []byte, end []byte) error {
	err := w.deleteRange(key, end)
	if err != nil {
		w.store.logger.Error("mvcc.Writer.DeleteRange() failed", "error", err)
		w.store.metrics.DeleteErrorsTotal.Inc()
		return fmt.Errorf("mvcc.Writer.DeleteRange() failed: %w", err)
	}
	return nil
}

func (w *writer) deleteRange(key []byte, end []byte) error {
	startRev := w.startRev.Main
	if len(w.changes) > 0 {
		startRev++
	}

	keys, revs := w.store.kvIndex.Range(key, end, startRev)
	if revs == nil {
		// this can mean either that there are no keys in the range, or that the range is compacted
		// we can distinguish these cases by checking if the startRev is less than the compacted rev
		// for now, leave it as a no-op
		return nil
		//return 0, errors.New("mvcc.Writer.DeleteRange() failed: error during index.Range(): no revisions returned")
	}

	for _, k := range keys {
		if err := w.deleteKey(k); err != nil {
			return fmt.Errorf("error during mvcc.Writer.DeleteRange(): error during mvcc.Writer.deleteKey(): %v", err)
		}
	}
	return nil
}

func (w *writer) DeleteKey(key []byte) error {
	return w.DeleteRange(key, nil)
}

func (w *writer) deleteKey(key []byte) error {
	nextRev := w.startRev.Main + 1
	bk := kv.NewKvBucketKey(nextRev, int64(len(w.changes)), true)
	// TODO: kv.EncodeRevisionAsBucketKey ??
	bkBytes := kv.NewRevBytes()
	bkBytes = kv.EncodeKvBucketKey(bk, bkBytes)

	// tombstone entry has a key but no value (api layer does not allow nil values)
	// modRev is bumped, so watchers dont see a sudden modRev == 0 after watching from lets say revs 5->8,
	// and then a put happens at rev 9, watchers couldve seen see 5,6,7,8,0 instead of 5,6,7,8,9(tombstone, since value == nil)
	tombstoneEntry := &kv.Entry{
		Key:    key,
		ModRev: nextRev,
	}
	tombstoneEntryBytes, err := kv.EncodeKvEntry(tombstoneEntry)
	if err != nil {
		return fmt.Errorf("writer.deleteKey(): %v", err)
	}

	// update history
	err = w.writeTx.UnsafePut(schema.BucketKV, bkBytes, tombstoneEntryBytes)
	if err != nil {
		return fmt.Errorf("writer.deleteKey(): failed to put tombstone entry: %v", err)
	}

	// update index
	err = w.store.kvIndex.Tombstone(key, bk.Revision)
	if err != nil {
		return fmt.Errorf("failed to tombstone key: %s, error: %v", string(key), err)
	}

	w.changes = append(w.changes, tombstoneEntry)
	return nil
}

func (w *writer) Abort() {
	w.changes = nil
	w.writeTx.Abort()
	w.writeTx.Unlock()       // release db lock
	w.store.rwlock.RUnlock() // release store lock
}

func (w *writer) End() error {
	defer func() {
		w.writeTx.Unlock()       // release db lock
		w.store.rwlock.RUnlock() // release store lock
	}()

	if len(w.changes) != 0 {
		w.store.revMu.Lock()
		w.store.currentRev = kv.Revision{Main: w.store.currentRev.Main + 1}
		w.writeTx.UnsafePut(schema.BucketMeta, schema.KeyCurrentRevision, util.EncodeUint64(uint64(w.store.currentRev.Main)))
	}

	if w.store.applyIndex > 0 {
		w.writeTx.UnsafePut(schema.BucketMeta, schema.KeyRaftApplyIndex, util.EncodeUint64(w.store.applyIndex))
		w.writeTx.UnsafePut(schema.BucketMeta, schema.KeyRaftTerm, util.EncodeUint64(w.store.raftTerm))
	}

	info, err := w.writeTx.Commit()
	if err != nil {
		msg := "failed to commit write tx"
		w.store.logger.Error(msg, "error", err)
		return fmt.Errorf("%s: %w", msg, err)
	} else {
		w.store.metrics.TxnsTotal.Add(1) // TODO: count failed txns?
		w.store.metrics.TxnDurationSec.Observe(time.Since(w.startTime).Seconds())
		w.store.metrics.PutsTotal.Add(float64(info.NewKeys))
		w.store.metrics.DeletesTotal.Add(float64(info.DeletedKeys))
		w.store.metrics.KeyCount.Add(float64(info.NewKeys - info.DeletedKeys))
	}

	if len(w.changes) != 0 {
		w.store.revMu.Unlock()
	}

	return nil
}
