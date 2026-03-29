package watch

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/types"
	"github.com/balits/kave/internal/util"
)

var (
	errFailureLimitReached = errors.New("watcher has reached failure limit")
)

type WatchHub struct {
	mu       sync.Mutex
	synced   map[int64]*watcher
	unsynced map[int64]*watcher
	store    mvcc.ReadOnlyStore
	logger   *slog.Logger
}

func (wh *WatchHub) NewWatcher(ctx context.Context, startRev int64, key, end []byte) *watcher {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	wh.logger.Info("new watcher", "start_rev", startRev, "key", key, "end", end)

	derivedCtx, cancel := context.WithCancel(ctx)

	w := &watcher{
		id:         util.NextNonNullID(),
		startRev:   startRev,
		currentRev: startRev,
		keyStart:   key,
		keyEnd:     end,
		ctx:        derivedCtx,
		cancel:     cancel,
		c:          make(chan Event, watcherChannelBufferSize),
	}

	currentRev, _ := wh.store.Revisions()
	if startRev >= currentRev.Main {
		wh.synced[w.id] = w
	} else {
		wh.unsynced[w.id] = w
	}

	return w
}

// unsafeSend tries to send all events to a group of watchers.
//
// If an error happens while sending the events to a watcher, it is marked for demotion if its overloaded,
// or deletion if another type of error occured.

// The onSendOverloadError and onSendOtherError callbacks are called after collecting each marked watcher, depending on what type of error it experienced.
// If a callback is nil, its considered a noop
func (wh *WatchHub) unsafeSend(group map[int64]*watcher, entries []types.KvEntry, onSendOverloadError func(wid int64), onSendOtherError func(wid int64)) {
	type removal struct {
		wid          int64
		cause        error
		demoteOrDrop bool // true -> demote, false -> drop
	}

	removals := make([]removal, 0)
	events := entriesToEvents(entries)

	for _, w := range group {
		err := w.sendAll(events)
		if err == nil {
			continue
		}

		rem := removal{
			cause: err,
			wid:   w.id,
			// default to Drop
		}
		if errors.Is(err, errWatcherOverloaded) {
			rem.demoteOrDrop = true
		}

		removals = append(removals, rem)
	}

	for _, r := range removals {
		if r.demoteOrDrop {
			wh.logger.Warn("watcher overloaded", "watcher_id", r.wid)
			if onSendOverloadError != nil {
				onSendOverloadError(r.wid)
			}
		} else {
			wh.logger.Warn("dropping watcher due to error sending event batch",
				"watcher_id", r.wid,
				"error", r.cause,
			)
			if onSendOtherError != nil {
				onSendOtherError(r.wid)
			}
		}
	}
}

func (wh *WatchHub) OnCommit(changes []types.KvEntry) {
	wh.mu.Lock()
	dropSynced := func(wid int64) { wh.unsafeDrop(wh.synced, wid) }
	wh.unsafeSend(wh.synced, changes, wh.unsafeDemote, dropSynced)
	wh.mu.Unlock()
}

func (wh *WatchHub) unsafeDrop(group map[int64]*watcher, wid int64) {
	wh.logger.Info("dropping watcher", "watcher_id", wid)
	w, ok := group[wid]
	if !ok {
		wh.logger.Warn("dropping watcher failed: watcher not found in group", "watcher_id", wid)
		return
	}
	w.close()
	delete(group, w.id)
}

func (wh *WatchHub) unsafeDemote(wid int64) {
	wh.logger.Info("demoting watcher to unsynced group", "watcher_id", wid)
	w, ok := wh.synced[wid]
	if !ok {
		wh.logger.Warn("watcher for demotion not found in synced group", "watcher_id", wid)
		return
	}
	delete(wh.synced, w.id)
	wh.unsynced[w.id] = w
}

func (wh *WatchHub) unsafePromote(w *watcher) {
	wh.logger.Info("promoting watcher to synced group", "watcher_id", w.id)
	if _, ok := wh.unsynced[w.id]; !ok {
		wh.logger.Warn("watcher for promotion not found in synced group", "watcher_id", w.id)
		return
	}
	delete(wh.unsynced, w.id)
	wh.synced[w.id] = w
}
