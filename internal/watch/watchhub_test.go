package watch

import (
	"testing"

	"github.com/balits/kave/internal/types"
	"github.com/stretchr/testify/require"
)

func Test_WatchHub_NewWatcher_PlacesSyncedWhenCaughtUp(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newTestHub(store)

	// startRev >= currentRev → synced
	w := hub.NewWatcher(t.Context(), 10, []byte("foo"), nil)

	require.Contains(t, hub.synced, w.id)
	require.NotContains(t, hub.unsynced, w.id)
}

func Test_WatchHub_NewWatcher_PlacesSyncedWhenAhead(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	// startRev > currentRev → also synced (watching future)
	w := hub.NewWatcher(t.Context(), 20, []byte("foo"), nil)

	require.Contains(t, hub.synced, w.id)
	require.NotContains(t, hub.unsynced, w.id)
}

func Test_WatchHub_NewWatcher_PlacesUnsyncedWhenBehind(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newTestHub(store)

	// startRev < currentRev → unsynced (needs catch-up)
	w := hub.NewWatcher(t.Context(), 3, []byte("foo"), nil)

	require.Contains(t, hub.unsynced, w.id)
	require.NotContains(t, hub.synced, w.id)
}

func Test_WatchHub_OnCommit_DeliversToSyncedWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)

	hub.OnCommit([]types.KvEntry{putEntry("foo", "val", 6)})

	got := expectEvents(t, w.c, 1)
	require.Equal(t, EventPut, got[0].Kind)
	require.Equal(t, "foo", string(got[0].Entry.Key))
	require.Equal(t, "val", string(got[0].Entry.Value))
}

func Test_WatchHub_OnCommit_DeliversDeleteEvent(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)

	hub.OnCommit([]types.KvEntry{tombstoneEntry("foo", 6)})

	got := expectEvents(t, w.c, 1)
	require.Equal(t, EventDelete, got[0].Kind)
	require.Equal(t, "foo", string(got[0].Entry.Key))
}

func Test_WatchHub_OnCommit_MultipleWatchers(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w1 := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)
	w2 := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)

	hub.OnCommit([]types.KvEntry{putEntry("foo", "v", 6)})

	got1 := expectEvents(t, w1.c, 1)
	got2 := expectEvents(t, w2.c, 1)
	require.Equal(t, "v", string(got1[0].Entry.Value))
	require.Equal(t, "v", string(got2[0].Entry.Value))
}

func Test_WatchHub_OnCommit_FiltersNonMatchingKeys(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	// w watches "foo", commit is for "bar"
	w := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)

	hub.OnCommit([]types.KvEntry{putEntry("bar", "v", 6)})

	expectNoEvents(t, w.c)
}

func Test_WatchHub_OnCommit_RangeWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	// [a, d)
	w := hub.NewWatcher(t.Context(), 5, []byte("a"), []byte("d"))

	hub.OnCommit([]types.KvEntry{
		putEntry("a", "v1", 6), // match
		putEntry("c", "v2", 7), // match
		putEntry("d", "v3", 8), // excluded (end is exclusive)
		putEntry("z", "v4", 9), // excluded
	})

	got := drainEvents(w.c)
	require.Len(t, got, 2)
	require.Equal(t, "a", string(got[0].Entry.Key))
	require.Equal(t, "c", string(got[1].Entry.Key))
}

func Test_WatchHub_OnCommit_SkipsUnsyncedWatchers(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newTestHub(store)

	// unsynced
	w := hub.NewWatcher(t.Context(), 3, []byte("foo"), nil)
	require.Contains(t, hub.unsynced, w.id)

	hub.OnCommit([]types.KvEntry{putEntry("foo", "v", 11)})

	// OnCommit only sends to synced
	expectNoEvents(t, w.c)
}

func Test_WatchHub_OnCommit_DemotesOverloadedWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w := newTestWatcher(t, []byte("foo"), nil, 5, nil, nil)
	close(w.c)
	w.c = make(chan Event, 1)
	hub.synced[w.id] = w

	hub.OnCommit([]types.KvEntry{putEntry("foo", "v1", 6)})

	// second commit should trigger overload
	hub.OnCommit([]types.KvEntry{putEntry("foo", "v2", 7)})

	require.NotContains(t, hub.synced, w.id)
	require.Contains(t, hub.unsynced, w.id)
}

func Test_WatchHub_OnCommit_DropsWatcherOnCancelledCtx(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)
	w.cancel()

	hub.OnCommit([]types.KvEntry{putEntry("foo", "v", 6)})

	require.NotContains(t, hub.synced, w.id)
	require.NotContains(t, hub.unsynced, w.id)
}

func Test_WatchHub_Demote_MovesSyncedToUnsynced(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)
	require.Contains(t, hub.synced, w.id)

	hub.mu.Lock()
	hub.unsafeDemote(w.id)
	hub.mu.Unlock()

	require.NotContains(t, hub.synced, w.id)
	require.Contains(t, hub.unsynced, w.id)
}

func Test_WatchHub_Promote_MovesUnsyncedToSynced(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newTestHub(store)

	w := hub.NewWatcher(t.Context(), 3, []byte("foo"), nil)
	require.Contains(t, hub.unsynced, w.id)

	hub.mu.Lock()
	hub.unsafePromote(w)
	hub.mu.Unlock()

	require.NotContains(t, hub.unsynced, w.id)
	require.Contains(t, hub.synced, w.id)
}

func Test_WatchHub_Drop_ClosesAndRemoves(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newTestHub(store)

	w := hub.NewWatcher(t.Context(), 5, []byte("foo"), nil)

	hub.mu.Lock()
	hub.unsafeDrop(hub.synced, w.id)
	hub.mu.Unlock()

	require.NotContains(t, hub.synced, w.id)
	require.Error(t, w.ctx.Err(), "context should be cancelled")

	_, ok := <-w.c
	require.False(t, ok, "channel should be closed")
}
