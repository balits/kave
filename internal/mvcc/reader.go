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
)

var (
	errRevisionLimitReached = errors.New("revision range: limit reached")
)

type Reader interface {
	SmartRevisionGetter

	SmartRevisionGetter

	// Range reads all entries in the [key, end) range at the given or current revision
	// up to the given limit (or no limit if limit == 0).
	// Additionally it returns the count of elements, and the last revison encountered scanning the elements.
	Range(key, end []byte, rev int64, limit int64) (entries []*kv.Entry, count int, lastRev int64, err error)

	// Get reads a single entry at the given or current revision.
	// Alias for Range(key, nil, 0, 1)
	Get(key []byte, rev int64) *kv.Entry
}

type reader struct {
	store   *KvStore
	metrics *metrics.KVMetrics
}

func (r *reader) Revisions() (current kv.Revision, compacted int64) {
	return r.store.Revisions()
}

func (r *reader) Range(key, end []byte, rev int64, limit int64) (entries []*kv.Entry, count int, highestRev int64, err error) {
	start := time.Now()
	r.store.rwlock.RLock()
	defer r.store.rwlock.RUnlock()

	r.metrics.ReadsTotal.Inc()
	defer func() { r.metrics.ReadDurationSec.Observe(time.Since(start).Seconds()) }()

	currRev, compactedRev := r.store.Revisions()
	rtx := r.store.backend.ReadTx()
	rtx.RLock()
	defer rtx.RUnlock()

	return doRange(r.store.logger, r.store.kvIndex, rtx, currRev.Main, compactedRev, rev, key, end, limit)
}

func (r *reader) Get(key []byte, rev int64) *kv.Entry {
	entries, _, _, err := r.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return entries[0]
}

// As this is an 'internal', non public facing API, we dont need to track metrics here
// its only used for the watch implementation, for which we will track different group of metrics
func (r *reader) RevisionRange(startRev, endRev int64, limit int64) (entries []*kv.Entry, err error) {
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
func doRange(l *slog.Logger, kvIndex kv.Index, readTx backend.ReadTx, currRev, compactedRev, targetRev int64, key, end []byte, limit int64) (entries []*kv.Entry, count int, lastRev int64, err error) {
	if targetRev > currRev {
		return nil, 0, 0, fmt.Errorf("future revision requested")
	}
	if targetRev <= 0 {
		targetRev = currRev
	}
	if targetRev < compactedRev {
		return nil, 0, 0, kv.ErrCompacted
	}

	revpairs, total := kvIndex.Revisions(key, end, targetRev, int(limit))
	if len(revpairs) == 0 {
		return nil, total, targetRev, nil
	}

	lim := int(limit)
	if lim <= 0 || lim > len(revpairs) {
		lim = len(revpairs)
	}

	entries = make([]*kv.Entry, 0, lim)
	revBytes := kv.NewRevBytes()
	for _, rp := range revpairs[:lim] {
		revBytes = kv.EncodeRevisionAsBucketKey(rp, revBytes)
		entryBytes, err := readTx.UnsafeGet(schema.BucketKV, revBytes)
		if err != nil || entryBytes == nil {
			l.Error("range: revision not found in backend", "main", rp.Main, "sub", rp.Sub)
			continue
		}

		entry, err := kv.DecodeEntry(entryBytes)
		if err != nil {
			l.Error("range: failed to unmarshal entry", "err", err)
			continue
		}
		if entry.ModRev > lastRev {
			lastRev = entry.ModRev
		}
		if entry.ModRev > lastRev {
			lastRev = entry.ModRev
		}
		entries = append(entries, entry)
	}

	return entries, total, lastRev, nil
}

// doRevisionRange performs a range query for all entries between the given revisions, returning up to the given limit of entries.
// It abstracts away the common logic of a revision range query, making it resuable for both Reader and Watcher implementations.
func doRevisionRange(l *slog.Logger, readTx backend.ReadTx, startRev, endRev, currRevMain, compactedRev, limit int64) (entries []*kv.Entry, err error) {
	if startRev > currRevMain {
		return nil, errors.New("revision range: future revision requested")
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

		entry, err := kv.DecodeEntry(v)
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
