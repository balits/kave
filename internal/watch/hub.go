package watch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	errFailureLimitReached = errors.New("watcher has reached failure limit")
)

const (
	defaultWatcherMapSize = 32
)

func NewWatchHub(reg prometheus.Registerer, logger *slog.Logger, store *mvcc.KvStore) *WatchHub {
	h := &WatchHub{
		synced:   make(map[int64]*watcher, defaultWatcherMapSize),
		unsynced: make(map[int64]*watcher, defaultWatcherMapSize),
		store:    store,
		logger:   logger.With("component", "watch_hub"),
	}
	h.metrics = metrics.NewWatchmetrics(reg, h)
	return h
}

// WatchHub is the central point of the watch API.
// All watches are created and canceled through the hub.
//
// Internally it manages a pool of "synced" watchers (whose revisions are caught up)
// and a pool of "unsynced" watchers (whos revisions are behind)
//
// It offers unsafe primitives to modify these pools, but higher level
// consumers of the hub should only rely of the safe functions, as they are safe
// to call concurrently.
type WatchHub struct {
	mu       sync.Mutex
	synced   map[int64]*watcher
	unsynced map[int64]*watcher
	store    mvcc.ReadOnlyStore
	metrics  *metrics.WatchMetrics // needs two phase init
	logger   *slog.Logger
}

// func (h *WatchHub) TestUnsafeSynced() map[int64]*watcher {
// 	if !testing.Testing() {
// 		panic("watch hub: TestUnsafeSynced() is only allowed in testing")
// 	}
// 	return h.synced
// }

// func (h *WatchHub) TestUnsafeUnsynced() map[int64]*watcher {
// 	if !testing.Testing() {
// 		panic("watch hub: TestUnsafeUnsynced() is only allowed in testing")
// 	}
// 	return h.unsynced
// }

func (h *WatchHub) TestSyncedCloned() map[int64]*watcher {
	if !testing.Testing() {
		panic("watch hub: TestSyncedCloned() is only allowed in testing")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	copy := make(map[int64]*watcher, len(h.synced))
	for k, v := range h.synced {
		if v == nil {
			copy[k] = nil
			continue
		}
		val := *v // deref to copy
		copy[k] = &val
	}
	return copy
}

func (h *WatchHub) TestUnsyncedCloned() map[int64]*watcher {
	if !testing.Testing() {
		panic("watch hub: TestUnsyncedCloned() is only allowed in testing")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	copy := make(map[int64]*watcher, len(h.unsynced))
	for k, v := range h.unsynced {
		if v == nil {
			copy[k] = nil
			continue
		}
		val := *v // deref to copy
		copy[k] = &val
	}
	return copy
}

func (h *WatchHub) Mu() *sync.Mutex {
	if !testing.Testing() {
		panic("watch hub: Mu() is only allowed in testing")
	}
	return &h.mu
}

// satisfy WatchMetrics

func (h *WatchHub) SyncedWatchesLen() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.synced)
}
func (h *WatchHub) UnsyncedWatchesLen() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.unsynced)
}

// NewWatcher creates a watcher based on the parameters, bounded by the given context.
// If the requested ID is zero, a random ID is generated,
// if theres an ID collision, an error is returned.
//
// If the startRev is less than the current store revision,
// the watch starts out in the unsynced group (see [UnsyncedLoop]).
// Otherwise it is treaded as synced, and will recieve events on commit.
func (h *WatchHub) NewWatcher(ctx context.Context, req api.WatchCreateRequest) (*watcher, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var id int64
	if req.WatchID == 0 {
		id = util.NextNonNullID()
	} else {
		id = req.WatchID
	}

	var key, end []byte
	if req.Key == nil {
		key = []byte{}
	} else {
		key = req.Key
	}

	end = req.End
	if req.Prefix {
		end = kv.PrefixEnd(req.Key)
	}

	currentRev, _, _, _ := h.store.Meta()
	// if start revision is 0, the user wants to watch from currentRev
	if req.StartRevision == 0 {
		req.StartRevision = currentRev.Main
	}

	// fix: "synced" means watcher is caught up to its keys revision, not to the global revision
	var lastRevision int64
	if end != nil {
		// range or prefix watch, default to main rev,
		// and unsynced loop will feed all needed events
		lastRevision = currentRev.Main
		fmt.Println("last rev for range/prefix is store rev:", currentRev.Main)
	} else {
		_, _, rev, err := h.store.NewReader().Range(req.Key, nil, 0, 0)
		fmt.Println("last rev for single key:", rev)
		if err != nil {
			h.logger.Error("new watcher failed",
				"error", ErrWatcherIDConflict.Error(),
			)
			return nil, err
		}

		lastRevision = rev
	}

	group := h.unsynced
	synced := false
	if req.StartRevision >= lastRevision {
		group = h.synced
		synced = true
	}

	if _, ok := group[id]; ok {
		h.logger.Error("new watcher failed",
			"error", ErrWatcherIDConflict.Error(),
			"id", id,
			"key", req.Key,
			"end", req.End,
			"start_rev", req.StartRevision,
			"up_to_date", synced,
		)
		return nil, ErrWatcherIDConflict
	}

	derivedCtx, cancel := context.WithCancelCause(ctx)
	w := &watcher{
		id:         id,
		startRev:   req.StartRevision,
		currentRev: req.StartRevision,
		keyStart:   key,
		keyEnd:     end,
		filter:     req.Filter,
		ctx:        derivedCtx,
		cancel:     cancel,
		prevEntry:  req.PrevEntry,
		c:          make(chan kv.Event, watcherChannelBufferSize),
	}
	group[w.id] = w

	h.logger.Info("new watcher",
		"id", id,
		"key", req.Key,
		"end", req.End,
		"start_rev", req.StartRevision,
		"up_to_date", synced,
	)

	return w, nil
}

type removal struct {
	wid          int64
	cause        error
	demoteOrDrop bool // true -> demote, false -> drop
}

// OnCommint sends all events to synced watchers.
//
// If an error happens while sending the events to a watcher, it is marked for demotion if its overloaded,
// or deletion if another type of error occured.
func (h *WatchHub) OnCommit(changes []*kv.Entry) {
	events := kv.ToKvEvents(changes)
	removals := make([]removal, 0)

	h.mu.Lock()
	defer h.mu.Unlock()

	start := time.Now()
	defer func() {
		if h.metrics != nil {
			h.metrics.OnCommitDurationSec.Observe(time.Since(start).Seconds())
		}
	}()

	for _, w := range h.synced {
		err := w.sendAll(events)
		if err == nil {
			continue
		}

		rem := removal{
			cause: err,
			wid:   w.id,
			// default to drop
		}
		if errors.Is(err, ErrWatcherOverloaded) {
			rem.demoteOrDrop = true
		}

		removals = append(removals, rem)
	}

	for _, r := range removals {
		if r.demoteOrDrop {
			h.logger.Info("demoting watcher due to overload", "watcher_id", r.wid)
			h.unsafeDemote(r.wid)
		} else {
			h.logger.Info("dropping watcher due to error sending event batch",
				"watcher_id", r.wid,
				"error", r.cause,
			)
			h.unsafeDropFromGroup(h.synced, r.wid, r.cause)
		}
	}
}

// DropWatcher removes the watcher from its group after closing it.
func (h *WatchHub) DropWatcher(wid int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, ok := h.synced[wid]
	if !ok {
		_, ok = h.unsynced[wid]
		if !ok {
			return false
		}
		h.unsafeDropFromGroup(h.unsynced, wid, context.Canceled)
	} else {
		h.unsafeDropFromGroup(h.synced, wid, context.Canceled)
	}

	return true
}

// unsafeDropWatcher removes the watcher from its group after closing it.
//
// NOTE: caller needs to hold the lock
func (h *WatchHub) unsafeDropWatcher(wid int64) {
	_, ok := h.synced[wid]
	if !ok {
		_, ok = h.unsynced[wid]
		if !ok {
			return
		}
		h.unsafeDropFromGroup(h.unsynced, wid, context.Canceled)
	} else {
		h.unsafeDropFromGroup(h.synced, wid, context.Canceled)
	}
}

// DropAll removes all the watchers from their respective groups, after closing them.
func (h *WatchHub) DropAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.logger.Info("Dropping all watchers from the store",
		"watcher_count", len(h.synced)+len(h.unsynced),
	)
	for id := range h.synced {
		h.unsafeDropFromGroup(h.synced, id, context.Canceled)
	}
	for id := range h.unsynced {
		h.unsafeDropFromGroup(h.unsynced, id, context.Canceled)
	}
}

// unsafeDrop removes the watcher from the group after closing
// its context and channel through watcher.close()
//
// NOTE: caller should hold the lock
func (h *WatchHub) unsafeDropFromGroup(group map[int64]*watcher, wid int64, cause error) {
	h.logger.Debug("dropping watcher", "watcher_id", wid, "cause", cause)
	w, ok := group[wid]
	if !ok {
		h.logger.Warn("dropping watcher failed: watcher not found in group", "watcher_id", wid)
		return
	}
	w.close(cause)
	delete(group, w.id)
}

// unsafeDemote moves the watch from the synced group to the unsynced group
//
// NOTE: caller should hold the lock
func (h *WatchHub) unsafeDemote(wid int64) {
	h.logger.Debug("demoting watcher to unsynced group", "watcher_id", wid)
	w, ok := h.synced[wid]
	if !ok {
		h.logger.Warn("watcher for demotion not found in synced group", "watcher_id", wid)
		return
	}
	delete(h.synced, w.id)
	h.unsynced[w.id] = w
}

// unsafePromote moves the watch from the unsynced group to the synced group
//
// NOTE: caller should hold the lock
func (h *WatchHub) unsafePromote(wid int64) {
	h.logger.Debug("promoting watcher to synced group", "watcher_id", wid)
	w, ok := h.unsynced[wid]
	if !ok {
		h.logger.Warn("watcher for promotion not found in synced group", "watcher_id", wid)
		return
	}
	delete(h.unsynced, w.id)
	h.synced[w.id] = w
}
