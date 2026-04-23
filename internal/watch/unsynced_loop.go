package watch

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
)

const (
	unsyncedWatcherLoopIntervalMs = 500
	unsynchedFailureLimit         = 5
	unsyncedBatchSize             = 32
)

// UnsyncedLoop is a background routine that manages the unsynced watchers,
// handles promotion and drops watchers who cannot progress.
//
// Internally it feeds data to the watchers until they are caught up.
// if they dont catch up in [unsynchedFailureLimit] rounds, they are dropped from the hub.
type UnsyncedLoop struct {
	hub              *WatchHub            // handle to the Hub
	rtx              backend.ReadTx       // handle to read transaction (for UnsafeGet/UnsafeScan)
	kvIndex          kv.Index             // revision index of each key, used to track which revisions the watcher needs to catch up
	storeMetaReader  mvcc.StoreMetaReader // for reading currentRev, compactedRev
	unsyncedBatch    map[int64]*watcher   // subset of watchers to feed data to, resets on each tick
	unsyncedFailures map[int64]int        // tracking failures for unsyncedBatch, incremented on err, deleted on drop/promote
	deleteBatch      map[int64]error      // batch of watchers marked for deletion, resets on each tick
	promoteBatch     map[int64]struct{}   // batch of watchers marked for promotion, resets on each tick
	ticker           util.Ticker          // ticker :D
	running          atomic.Bool          // atomic flag for running state
	ctx              context.Context      // top level ctx, canceled when stopped
	cancel           context.CancelFunc   // cancel to the ctx
	logger           slog.Logger          // and perhaps a logger
}

func NewUnsyncedLoop(logger *slog.Logger, wh *WatchHub, kvIndex kv.Index, readTx backend.ReadTx, storeMetaReader mvcc.StoreMetaReader) *UnsyncedLoop {
	return &UnsyncedLoop{
		hub:              wh,
		rtx:              readTx,
		kvIndex:          kvIndex,
		storeMetaReader:  storeMetaReader,
		ticker:           util.NewRealTicker(unsyncedWatcherLoopIntervalMs),
		unsyncedFailures: make(map[int64]int),
		unsyncedBatch:    make(map[int64]*watcher, unsyncedBatchSize),
		deleteBatch:      make(map[int64]error),
		promoteBatch:     make(map[int64]struct{}),
		logger:           *logger.With("component", "unsynced_watcher_loop"),
	}
}

func (u *UnsyncedLoop) Run(ctx context.Context) {
	if !u.running.CompareAndSwap(false, true) {
		u.logger.Warn("Attempted to run unsynced watcher loop while it was already running")
		return
	}
	u.logger.Info("Unsynced watcher loop started")
	ctx, cancel := context.WithCancel(ctx)
	u.ctx = ctx
	u.cancel = cancel
	u.run()
}

func (u *UnsyncedLoop) Stop() {
	u.logger.Info("Stopping unsynced watcher loop")
	if u.cancel != nil {
		u.cancel()
	}
	u.running.Store(false)
}

func (u *UnsyncedLoop) run() {
	for {
		select {
		case <-u.ctx.Done():
			u.logger.Info("context cancelled, stopping main loop", "cause", u.ctx.Err())
			return
		case <-u.ticker.Tick():
			u.tick()
		}
	}
}

// tick represents one tick of the loop. It creates a subset of the unsynced watchers,
// and tries to feed them the revisions they lack. If that fails, their failure count
// gets incremented, and once enough failures have accumulated, they will be dropped.
//
// Internally it uses the inmemory kv index to keep track of revisions, and a handle to
// a read transaction from the backend to get the entries from the kv bucket.
func (u *UnsyncedLoop) tick() {
	u.hub.mu.Lock()
	for id, w := range u.hub.unsynced {
		if len(u.unsyncedBatch) < unsyncedBatchSize {
			u.unsyncedBatch[id] = w
		} else {
			break
		}
	}
	u.hub.mu.Unlock()

	storeCurrentRev, compactedRev := u.storeMetaReader.Revisions()

OUTER:
	for _, w := range u.unsyncedBatch {
		if u.unsyncedFailures[w.id] >= unsynchedFailureLimit || w.ctx.Err() != nil {
			u.deleteBatch[w.id] = errFailureLimitReached
			u.logger.Debug("marking watch for deletion",
				"watcher_id", w.id,
				"cause", errFailureLimitReached,
			)
			continue
		}

		if w.currentRev < compactedRev {
			// revs are compacted away
			// u.incFailures(w) // compacted rev aint gonne get any smaller, so mark for deletion
			u.deleteBatch[w.id] = kv.ErrCompacted
			continue
		}

		revsAsBucketKeys := u.kvIndex.RevisionsRange(w.keyStart, w.keyEnd, w.currentRev, storeCurrentRev.Main+1)
		// fast forwads watchers who has not seen any updates since they currentRev
		if len(revsAsBucketKeys) == 0 {
			w.currentRev = storeCurrentRev.Main
			u.promoteBatch[w.id] = struct{}{}
			continue OUTER
		}

		events := make([]kv.Event, len(revsAsBucketKeys))
		u.rtx.RLock()
		for i, bucketKey := range revsAsBucketKeys {
			bk := kv.EncodeKvBucketKey(bucketKey, kv.NewRevBytes())
			val, err := u.rtx.UnsafeGet(schema.BucketKV, bk)
			if err != nil || val == nil {
				u.logger.Debug("failed to syncing watcher: failed to read revision from backend",
					"error", err,
					"key_start", w.keyStart,
					"key_end", w.keyEnd,
					"revision", bucketKey,
				)
				u.incFailures(w)
				u.rtx.RUnlock()
				continue OUTER
			}

			entry, err := kv.DecodeEntry(val)
			if err != nil {
				u.logger.Debug("failed to syncing watcher: failed to decode entry",
					"error", err,
					"key_start", w.keyStart,
					"key_end", w.keyStart,
					"revision", bucketKey,
				)
				u.incFailures(w)
				u.rtx.RUnlock()
				continue OUTER
			}

			events[i] = kv.ToKvEvent(entry)
		}
		u.rtx.RUnlock()

		err := w.sendAll(events)
		if errors.Is(err, context.Canceled) {
			u.deleteBatch[w.id] = context.Canceled
			u.logger.Info("marking watch for deletion",
				"watcher_id", w.id,
				"cause", context.Canceled,
			)
			continue
		} else if errors.Is(err, ErrWatcherOverloaded) {
			u.logger.Warn("failed to syncing watcher: watcher overloaded")
			u.incFailures(w)
			continue
		}

		highestRev := events[len(events)-1].Entry.ModRev
		if w.currentRev >= highestRev {
			u.promoteBatch[w.id] = struct{}{}
		}
	}

	u.hub.mu.Lock()
	for wid, cause := range u.deleteBatch {
		u.unsafeDropFailedWatcher(wid, cause)
		delete(u.unsyncedFailures, wid)
	}
	for wid := range u.promoteBatch {
		u.hub.unsafePromote(wid)
		delete(u.unsyncedFailures, wid)
	}
	u.hub.mu.Unlock()

	clear(u.unsyncedBatch)
	clear(u.deleteBatch)
	clear(u.promoteBatch)
}

func (u *UnsyncedLoop) incFailures(w *watcher) {
	if _, ok := u.unsyncedFailures[w.id]; !ok {
		u.unsyncedFailures[w.id] = 0
	}
	u.unsyncedFailures[w.id]++
}

func (u *UnsyncedLoop) unsafeDropFailedWatcher(wid int64, cause error) {
	w, ok := u.hub.unsynced[wid]
	if !ok {
		u.logger.Warn("attempted to drop failed unsynced watcher that is not tracked", "watcher_id", wid)
		return
	}

	u.hub.logger.Warn("dropping unsynced watcher due to excessive send failures",
		"watcher_id", w.id,
		"failure_count", u.unsyncedFailures[w.id],
		"cause", cause,
	)
	u.hub.unsafeDropFromGroup(u.hub.unsynced, w.id)
	delete(u.unsyncedFailures, w.id)
}
