package watch

import (
	"context"
	"errors"
	"log/slog"

	"github.com/balits/kave/internal/types"
	"github.com/balits/kave/internal/util"
)

const (
	unsyncedWatcherLoopIntervalMs = 500
	unsynchedFailureLimit         = 5
)

func NewUnsyncedWatcherLoop(logger *slog.Logger, wd *WatchHub) *UnsyncedWatcherLoop {
	return &UnsyncedWatcherLoop{
		wh:               wd,
		ticker:           util.NewRealTicker(unsyncedWatcherLoopIntervalMs),
		unsyncedFailures: make(map[int64]int, len(wd.unsynced)),
		logger:           *logger.With("component", "unsynced_watcher_loop"),
	}
}

type UnsyncedWatcherLoop struct {
	wh               *WatchHub
	unsyncedFailures map[int64]int
	ticker           util.Ticker
	ctx              context.Context
	cancel           context.CancelFunc
	logger           slog.Logger
}

func (u *UnsyncedWatcherLoop) Run(ctx context.Context) {
	u.logger.Info("starting unsynced watcher loop")
	ctx, cancel := context.WithCancel(ctx)
	u.ctx = ctx
	u.cancel = cancel
	u.run()
}

func (u *UnsyncedWatcherLoop) run() {
	for {
		select {
		case <-u.ctx.Done():
			u.logger.Info("stopping unsynced watcher loop", "cause", u.ctx.Err())
			return
		case <-u.ticker.Tick():
			u.tick()
		}
	}
}
func (u *UnsyncedWatcherLoop) tick() {
	u.wh.mu.Lock()
	defer u.wh.mu.Unlock()

	batchLimit := 5
	watcherBatch := make(map[int64]*watcher, batchLimit)
	for id, w := range u.wh.unsynced {
		if len(watcherBatch) < batchLimit {
			watcherBatch[id] = w
		} else {
			break
		}
	}

	storeCurrentRev, _ := u.wh.store.Revisions()

	for _, w := range watcherBatch {
		if u.unsyncedFailures[w.id] >= unsynchedFailureLimit || w.ctx.Err() != nil {
			u.dropFailedWatcher(w.id, errFailureLimitReached)
			continue
		}

		r := u.wh.store.NewReader()
		entries, err := r.RevisionRange(w.currentRev+1, storeCurrentRev.Main, 0)
		if err != nil {
			u.logger.Error("error syncing watcher", "error", err)
			u.incFailures(w)
			continue
		}

		err = w.sendAll(entriesToEvents(entries))

		if err == nil && w.currentRev >= storeCurrentRev.Main {
			u.wh.unsafePromote(w)
			delete(u.unsyncedFailures, w.id)
			continue
		}

		if errors.Is(err, context.Canceled) {
			u.dropFailedWatcher(w.id, context.Canceled)
		} else if errors.Is(err, errWatcherOverloaded) {
			u.incFailures(w)
		}
	}
}

func (u *UnsyncedWatcherLoop) incFailures(w *watcher) {
	if _, ok := u.unsyncedFailures[w.id]; !ok {
		u.unsyncedFailures[w.id] = 0
	}
	u.unsyncedFailures[w.id]++
}

func (u *UnsyncedWatcherLoop) dropFailedWatcher(wid int64, cause error) {
	w, ok := u.wh.unsynced[wid]
	if !ok {
		u.logger.Warn("attempted to drop failed unsynced watcher that is not tracked", "watcher_id", wid)
		return
	}

	u.wh.logger.Warn("dropping watcher due to excessive unsynced send failures",
		"watcher_id", w.id,
		"failure_count", u.unsyncedFailures[w.id],
		"cause", cause,
	)
	u.wh.unsafeDrop(u.wh.unsynced, w.id)
	delete(u.unsyncedFailures, w.id)
}

func entriesToEvents(in []types.KvEntry) (out []Event) {
	out = make([]Event, len(in))
	for i, e := range in {
		out[i] = entryToEvent(e)
	}
	return
}

func entryToEvent(in types.KvEntry) Event {
	if in.Tombstone() {
		return Event{
			Kind:  EventDelete,
			Entry: in,
		}
	}
	return Event{
		Kind:  EventPut,
		Entry: in,
	}
}

// TODO(2): how to use SSE in general?
type WatchStreamSSE struct {
	watchers []*watcher
}
