package watch

import (
	"context"
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/types"
	"github.com/stretchr/testify/require"
)

func Test_UnsyncedLoop_CatchesUpAndPromotes(t *testing.T) {
	// Store has data at revisions 1-5
	// Watcher starts at rev 1, so it needsto catch up on revisions 2-5
	// before it can be promoted
	entries := []types.KvEntry{
		putEntry("foo", "v2", 2),
		putEntry("foo", "v3", 3),
		putEntry("foo", "v4", 4),
		putEntry("foo", "v5", 5),
	}

	store := &mockStore{
		currentRev: 5,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 1, []byte("foo"), nil)
	require.Contains(t, hub.unsynced, w.id, "watcher should start unsynced")

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.NotContains(t, hub.unsynced, w.id, "watcher should be removed from unsynced after catch-up")
	require.Contains(t, hub.synced, w.id, "watcher should be promoted to synced after catch-up")

	// verify events
	got := drainEvents(w.c)
	require.Len(t, got, 4)
	require.Equal(t, "v2", string(got[0].Entry.Value))
	require.Equal(t, "v5", string(got[3].Entry.Value))

	// verify currentRev
	require.Equal(t, int64(5), w.currentRev)
}

func Test_UnsyncedLoop_PartialCatchUpStaysUnsynced(t *testing.T) {
	// Reader returns entries only up to rev 5, but store is at rev 10
	// Watcher starts at rev 1, so after one tick its at rev 5
	// since its still behind, it should stay in unsynced
	entries := []types.KvEntry{
		putEntry("foo", "v2", 2),
		putEntry("foo", "v3", 3),
		putEntry("foo", "v5", 5),
	}

	store := &mockStore{
		currentRev: 10,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 1, []byte("foo"), nil)

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.Contains(t, hub.unsynced, w.id, "watcher should remain unsynced when not caught up")
	require.NotContains(t, hub.synced, w.id)
	require.Equal(t, int64(5), w.currentRev)
}

func Test_UnsyncedLoop_CompactionErrorIncrementsFailures(t *testing.T) {
	store := &mockStore{
		currentRev: 10,
		readerFn: func() mvcc.Reader {
			return &mockReader{err: kv.ErrCompacted}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 1, []byte("foo"), nil)

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.Contains(t, hub.unsynced, w.id)
	require.Equal(t, 1, loop.unsyncedFailures[w.id])

	require.Empty(t, drainEvents(w.c))
}

func Test_UnsyncedLoop_DropsWatcherAfterMaxFailures(t *testing.T) {
	store := &mockStore{
		currentRev: 10,
		readerFn: func() mvcc.Reader {
			return &mockReader{err: kv.ErrCompacted}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 1, []byte("foo"), nil)

	loop := newTestUnsyncedLoop(hub)

	for range unsynchedFailureLimit {
		loop.tick()
	}
	require.Contains(t, hub.unsynced, w.id, "watcher should survive up to the limit")
	require.Equal(t, unsynchedFailureLimit, loop.unsyncedFailures[w.id])

	// one more tick should trigger the drop
	loop.tick()

	require.NotContains(t, hub.unsynced, w.id, "watcher should be dropped after exceeding failure limit")
	require.NotContains(t, loop.unsyncedFailures, w.id, "failure counter should be cleaned up")

	require.Error(t, w.ctx.Err())
	_, ok := <-w.c
	require.False(t, ok)
}

func Test_UnsyncedLoop_DropsWatcherOnContextCancel(t *testing.T) {
	store := &mockStore{
		currentRev: 10,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: []types.KvEntry{putEntry("foo", "v", 5)}}
		},
	}

	hub := newTestHub(store)
	ctx, cancel := context.WithCancel(t.Context())
	w := hub.NewWatcher(ctx, 1, []byte("foo"), nil)

	cancel()

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.NotContains(t, hub.unsynced, w.id,
		"watcher with cancelled context should be dropped")
}

func Test_UnsyncedLoop_OverloadIncrementsFailureButKeepsWatcher(t *testing.T) {
	var entries []types.KvEntry
	for i := int64(2); i <= 200; i++ {
		entries = append(entries, putEntry("foo", "v", i))
	}

	store := &mockStore{
		currentRev: 200,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)

	w := newTestWatcher(t, []byte("foo"), nil, 1, nil, nil)
	close(w.c)
	w.c = make(chan Event, 2)
	hub.unsynced[w.id] = w

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.Contains(t, hub.unsynced, w.id,
		"overloaded watcher should NOT be dropped on first failure")
	require.Equal(t, 1, loop.unsyncedFailures[w.id])
}

func Test_UnsyncedLoop_ClearsFailureCounterOnPromotion(t *testing.T) {
	store := &mockStore{
		currentRev: 5,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: []types.KvEntry{
				putEntry("foo", "v", 5),
			}}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 4, []byte("foo"), nil)

	loop := newTestUnsyncedLoop(hub)
	// seed prior failure
	loop.unsyncedFailures[w.id] = 2

	loop.tick()

	require.Contains(t, hub.synced, w.id)
	require.NotContains(t, loop.unsyncedFailures, w.id,
		"failure counter should be deleted on successful promotion")
}

func Test_UnsyncedLoop_Integration_AfterPromotion_OnCommitDelivers(t *testing.T) {
	entries := []types.KvEntry{
		putEntry("foo", "catch-up", 3),
	}

	store := &mockStore{
		currentRev: 3,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 2, []byte("foo"), nil)

	// catch up and promote
	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.Contains(t, hub.synced, w.id, "watcher should be promoted")

	// drain the catch-up events
	catchUp := drainEvents(w.c)
	require.Len(t, catchUp, 1)
	require.Equal(t, "catch-up", string(catchUp[0].Entry.Value))

	// new write comes through synced
	store.setCurrentRev(4)
	hub.OnCommit([]types.KvEntry{putEntry("foo", "live", 4)})

	live := expectEvents(t, w.c, 1)
	require.Equal(t, "live", string(live[0].Entry.Value))
}

func Test_UnsyncedLoop_Integration_NoDoubleDeliveryOnPromotion(t *testing.T) {
	// The loop catches up through rev 5 and promotes
	// OnCommit fires with the same rev 5 entries (simulating race where the commit happened while catch up was in progress)
	// The idempotent check in send() should prevent duplicates.
	entries := []types.KvEntry{
		putEntry("foo", "v4", 4),
		putEntry("foo", "v5", 5),
	}

	store := &mockStore{
		currentRev: 5,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 3, []byte("foo"), nil)

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	require.Contains(t, hub.synced, w.id, "watcher should be promoted")
	require.Equal(t, int64(5), w.currentRev)

	hub.OnCommit([]types.KvEntry{putEntry("foo", "v5-dup", 5)})

	all := drainEvents(w.c)

	require.Len(t, all, 2,
		"idempotent check should prevent double delivery at promotion boundary")
	require.Equal(t, "v4", string(all[0].Entry.Value))
	require.Equal(t, "v5", string(all[1].Entry.Value))
}


func Test_UnsyncedLoop_EmptyGroupNoOp(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	loop := newTestUnsyncedLoop(hub)

	require.NotPanics(t, func() { loop.tick() })
}
func Test_UnsyncedLoop_ProcessesMultipleWatchers(t *testing.T) {
	entries := []types.KvEntry{
		putEntry("a", "va", 3),
		putEntry("b", "vb", 4),
	}

	store := &mockStore{
		currentRev: 4,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)

	// both behind
	w1 := hub.NewWatcher(t.Context(), 2, []byte("a"), nil)
	w2 := hub.NewWatcher(t.Context(), 2, []byte("b"), nil)

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	// both should be promoted
	require.Contains(t, hub.synced, w1.id)
	require.Contains(t, hub.synced, w2.id)

	got1 := drainEvents(w1.c)
	got2 := drainEvents(w2.c)

	require.Len(t, got1, 1)
	require.Equal(t, "a", string(got1[0].Entry.Key))

	require.Len(t, got2, 1)
	require.Equal(t, "b", string(got2[0].Entry.Key))
}

func Test_UnsyncedLoop_TombstoneBecomesDeleteEvent(t *testing.T) {
	entries := []types.KvEntry{
		putEntry("foo", "alive", 2),
		tombstoneEntry("foo", 3),
	}

	store := &mockStore{
		currentRev: 3,
		readerFn: func() mvcc.Reader {
			return &mockReader{entries: entries}
		},
	}

	hub := newTestHub(store)
	w := hub.NewWatcher(t.Context(), 1, []byte("foo"), nil)

	loop := newTestUnsyncedLoop(hub)
	loop.tick()

	got := drainEvents(w.c)
	require.Len(t, got, 2)
	require.Equal(t, EventPut, got[0].Kind)
	require.Equal(t, EventDelete, got[1].Kind)
}
