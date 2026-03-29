package mvcc

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types"
)

var (
	errRevisionLimitReached = errors.New("revision range: limit reached")
)

type Reader interface {
	// Range reads all entries in the [key, end) range at the given or current revision
	//  up to the given limit.
	Range(key, end []byte, rev int64, limit int64) (entries []types.KvEntry, count int, currRev int64, err error)

	// Get reads a single entry at the given or current revision.
	Get(key []byte, rev int64) *types.KvEntry

	// RevisionRange reads all entries between the given revisions [startRev, endRev)
	// exclusive of the end revision.
	RevisionRange(startRev, endRev int64, limit int64) (entries []types.KvEntry, err error)
}

type reader struct {
	store   *KvStore
	metrics *metrics.KVMetrics
}

func (r *reader) Range(key, end []byte, rev int64, limit int64) (entries []types.KvEntry, count int, currentRev int64, err error) {
	start := time.Now()
	r.store.rwlock.RLock()
	defer r.store.rwlock.RUnlock()

	r.metrics.ReadsTotal.Inc()
	defer func() { r.metrics.ReadDurationSec.Observe(time.Since(start).Seconds()) }()

	currRev, compactedRev := r.store.Revisions()
	rtx := r.store.backend.ReadTx()
	rtx.RLock()
	defer rtx.RUnlock()

	entries, count, err = doRange(r.store.logger, r.store.kvIndex, rtx, currRev.Main, compactedRev, rev, key, end, limit)
	if err != nil {
		return nil, 0, currRev.Main, err
	}
	return entries, count, currRev.Main, nil
}

func (r *reader) Get(key []byte, rev int64) *types.KvEntry {
	entries, _, _, err := r.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return &entries[0]
}

// As this is an 'internal', non public facing API, we dont need to track metrics here
// its only used for the watch implementation, for which we will track different group of metrics
func (r *reader) RevisionRange(startRev, endRev int64, limit int64) (entries []types.KvEntry, err error) {
	r.store.rwlock.RLock()
	defer r.store.rwlock.RUnlock()

	currRev, compactRev := r.store.Revisions()

	rtx := r.store.backend.ReadTx()
	rtx.RLock()
	defer rtx.RUnlock()
	return doRevisionRange(r.store.logger, rtx, startRev, endRev, currRev.Main, compactRev, limit)
}

// doRange performs a range query for the given key range and revision, returning up to the given limit of entries.
// It abstracts away the common logic of a range query, making it resuable for both Reader and Watcher implementations.
func doRange(l *slog.Logger, kvIndex kv.Index, readTx backend.ReadTx, currRev, compactedRev, targetRev int64, key, end []byte, limit int64) (entries []types.KvEntry, count int, err error) {
	if targetRev > currRev {
		return nil, 0, fmt.Errorf("future revision requested")
	}
	if targetRev <= 0 {
		targetRev = currRev
	}
	if targetRev < compactedRev {
		return nil, 0, kv.ErrCompacted
	}

	revpairs, total := kvIndex.Revisions(key, end, targetRev, int(limit))
	if len(revpairs) == 0 {
		return nil, total, nil
	}

	lim := int(limit)
	if lim <= 0 || lim > len(revpairs) {
		lim = len(revpairs)
	}

	entries = make([]types.KvEntry, 0, lim)
	revBytes := kv.NewRevBytes()
	for _, rp := range revpairs[:lim] {
		revBytes = kv.EncodeRevisionAsBucketKey(rp, revBytes)
		entryBytes, err := readTx.UnsafeGet(schema.BucketKV, revBytes)
		if err != nil || entryBytes == nil {
			l.Error("range: revision not found in backend", "main", rp.Main, "sub", rp.Sub)
			continue
		}
		entry, err := types.DecodeKvEntry(entryBytes)
		if err != nil {
			l.Error("range: failed to unmarshal entry", "err", err)
			continue
		}
		entries = append(entries, entry)
	}

	return entries, total, nil
}

// doRevisionRange performs a range query for all entries between the given revisions, returning up to the given limit of entries.
// It abstracts away the common logic of a revision range query, making it resuable for both Reader and Watcher implementations.
func doRevisionRange(l *slog.Logger, readTx backend.ReadTx, startRev, endRev, currRevMain, compactedRev, limit int64) (entries []types.KvEntry, err error) {
	if startRev > currRevMain {
		return nil, errors.New("revision range: future revision requested")
	}
	if startRev <= 0 {
		startRev = 0
	}
	if startRev < compactedRev {
		return nil, kv.ErrCompacted
	}

	if limit <= 0 {
		limit = math.MaxInt64
	}

	// make end key exclusive
	if endRev <= 0 {
		// bug: since end is exclusive, the very last last revision will not be scanned
		// but also: when will we ever have 9223372036854775807 revisions?
		endRev = math.MaxInt64
	} else if endRev < startRev {
		return nil, fmt.Errorf("revision range: invalid range where endRev < startRev, range: [%d,%d)", startRev, endRev)
	} else if endRev < math.MaxInt64 {
		endRev++
	}

	startInclusive := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: startRev, Sub: 0}, kv.NewRevBytes())
	endExclusive := kv.EncodeRevisionAsBucketKey(kv.Revision{Main: endRev, Sub: 0}, kv.NewRevBytes())

	err = readTx.UnsafeScan(schema.BucketKV, startInclusive, endExclusive, func(k, v []byte) error {
		// bk := kv.DecodeKvBucketKey(k)
		// we can figure out if key is tombstoned, while having the modRev updated (fixed in w.deleteKey)

		entry, err := types.DecodeKvEntry(v)
		if err != nil {
			// we can skip entries that fail to decode, as they might be due to
			// compaction or other issues, but we dont want to fail
			// the entire request because of that
			l.Error("revision range: failed to decode entry", "err", err)
			return nil
		}
		if len(entries) < int(limit) {
			entries = append(entries, entry)
		} else if limit != 0 {
			return errRevisionLimitReached
		}
		return nil
	})

	if err != nil && !errors.Is(err, errRevisionLimitReached) {
		return nil, err
	}

	return entries, nil
}
