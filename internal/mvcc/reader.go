package mvcc

import (
	"fmt"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/types"
)

type Reader interface {
	// Range reads entries at the given or current revision.
	Range(key, end []byte, rev int64, limit int64) (entries []types.Entry, count int, currRev int64, err error)

	// Get reads a single entry at the given or current revision.
	Get(key []byte, rev int64) *types.Entry
}

type reader struct {
	store   *KVStore
	metrics *metrics.KVMetrics
}

func (r *reader) Range(key, end []byte, rev int64, limit int64) (entries []types.Entry, count int, currentRev int64, err error) {
	start := time.Now()
	r.store.rwlock.RLock()

	r.store.revMu.RLock()
	currRevMain := r.store.currentRev.Main
	compactRev := r.store.compactedMainRev
	r.store.revMu.RUnlock()

	defer r.store.rwlock.RUnlock()
	r.metrics.ReadsTotal.Inc()
	defer r.metrics.ReadDurationSec.Observe(time.Since(start).Seconds())

	if rev > currRevMain {
		return nil, 0, currRevMain, fmt.Errorf("future revision requested")
	}
	if rev <= 0 {
		rev = currRevMain
	}
	if rev < compactRev {
		return nil, 0, 0, kv.ErrCompacted
	}

	revpairs, total := r.store.kvIndex.Revisions(key, end, rev, int(limit))
	if len(revpairs) == 0 {
		return nil, total, currRevMain, nil
	}

	lim := int(limit)
	if lim <= 0 || lim > len(revpairs) {
		lim = len(revpairs)
	}

	rtx := r.store.backend.ReadTx()
	rtx.RLock()
	defer rtx.RUnlock()

	entries = make([]types.Entry, 0, lim)
	revBytes := kv.NewRevBytes()
	for _, rp := range revpairs[:lim] {
		revBytes = kv.EncodeRevision(rp, revBytes)
		entryBytes, err := rtx.UnsafeGet(schema.BucketKV, revBytes)
		if err != nil || entryBytes == nil {
			r.store.logger.Error("range: revision not found in backend", "main", rp.Main, "sub", rp.Sub)
			continue
		}
		entry, err := types.DecodeEntry(entryBytes)
		if err != nil {
			r.store.logger.Error("range: failed to unmarshal entry", "err", err)
			continue
		}
		entries = append(entries, entry)
	}

	return entries, total, currRevMain, nil
}

func (r *reader) Get(key []byte, rev int64) *types.Entry {
	r.store.rwlock.RLock()
	defer r.store.rwlock.RUnlock()
	entries, _, _, err := r.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return &entries[0]
}
