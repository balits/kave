package watch

import (
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/types/api"
	"github.com/stretchr/testify/require"
)

func Test_WatchHub_NewWatcher_PlacesSyncedWhenCaughtUp(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newMockHub(store)

	// startRev >= currentRev → synced
	req := api.WatchCreateRequest{StartRevision: 10, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	require.Contains(t, hub.synced, w.id)
	require.NotContains(t, hub.unsynced, w.id)
}

func Test_WatchHub_NewWatcher_PlacesSyncedWhenAhead(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	// startRev > currentRev → also synced (watching future)
	req := api.WatchCreateRequest{StartRevision: 20, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	require.Contains(t, hub.synced, w.id)
	require.NotContains(t, hub.unsynced, w.id)
}

func Test_WatchHub_NewWatcher_PlacesUnsyncedWhenBehind(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newMockHub(store)

	// startRev < currentRev → unsynced (needs catch-up)
	req := api.WatchCreateRequest{StartRevision: 3, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	require.Contains(t, hub.unsynced, w.id)
	require.NotContains(t, hub.synced, w.id)
}

func Test_WatchHub_OnCommit_DeliversToSyncedWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	hub.OnCommit([]*kv.Entry{putEntry("foo", "val", 6)})

	got := expectEvents(t, w.c, 1)
	require.Equal(t, kv.EventPut, got[0].Kind)
	require.Equal(t, "foo", string(got[0].Entry.Key))
	require.Equal(t, "val", string(got[0].Entry.Value))
}

func Test_WatchHub_OnCommit_DeliversDeleteEvent(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	hub.OnCommit([]*kv.Entry{testTombstoneEntry("foo", 6)})

	got := expectEvents(t, w.c, 1)
	require.Equal(t, kv.EventDelete, got[0].Kind)
	require.Equal(t, "foo", string(got[0].Entry.Key))
}

func Test_WatchHub_OnCommit_MultipleWatchers(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req1 := api.WatchCreateRequest{WatchID: 1, StartRevision: 5, Key: []byte("foo")}
	w1, err := hub.NewWatcher(t.Context(), req1)
	require.NoError(t, err)
	req2 := api.WatchCreateRequest{WatchID: 2, StartRevision: 5, Key: []byte("foo")}
	w2, err := hub.NewWatcher(t.Context(), req2)
	require.NoError(t, err)

	hub.OnCommit([]*kv.Entry{putEntry("foo", "v", 6)})

	got1 := expectEvents(t, w1.c, 1)
	got2 := expectEvents(t, w2.c, 1)
	require.Equal(t, "v", string(got1[0].Entry.Value))
	require.Equal(t, "v", string(got2[0].Entry.Value))
}

func Test_WatchHub_OnCommit_FiltersNonMatchingKeys(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	// w watches "foo", commit is for "bar"
	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	hub.OnCommit([]*kv.Entry{putEntry("bar", "v", 6)})

	expectNoEvents(t, w.c)
}

func Test_WatchHub_OnCommit_RangeWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	// [a, d)
	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("a"), End: []byte("d")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	hub.OnCommit([]*kv.Entry{
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
	hub := newMockHub(store)

	// unsynced
	req := api.WatchCreateRequest{StartRevision: 3, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	require.Contains(t, hub.unsynced, w.id)

	hub.OnCommit([]*kv.Entry{putEntry("foo", "v", 11)})

	// OnCommit only sends to synced
	expectNoEvents(t, w.c)
}

func Test_WatchHub_OnCommit_DemotesOverloadedWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	w := newTestWatcher(t, []byte("foo"), nil, 5, nil, nil)
	close(w.c)
	w.c = make(chan kv.Event, 1)
	hub.synced[w.id] = w

	hub.OnCommit([]*kv.Entry{putEntry("foo", "v1", 6)})

	// second commit should trigger overload
	hub.OnCommit([]*kv.Entry{putEntry("foo", "v2", 7)})

	require.NotContains(t, hub.synced, w.id)
	require.Contains(t, hub.unsynced, w.id)
}

func Test_WatchHub_OnCommit_DropsWatcherOnCancelledCtx(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	w.cancel()

	hub.OnCommit([]*kv.Entry{putEntry("foo", "v", 6)})

	require.NotContains(t, hub.synced, w.id)
	require.NotContains(t, hub.unsynced, w.id)
}

func Test_WatchHub_Demote_MovesSyncedToUnsynced(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	require.Contains(t, hub.synced, w.id)

	hub.mu.Lock()
	hub.unsafeDemote(w.id)
	hub.mu.Unlock()

	require.NotContains(t, hub.synced, w.id)
	require.Contains(t, hub.unsynced, w.id)
}

func Test_WatchHub_Promote_MovesUnsyncedToSynced(t *testing.T) {
	store := &mockStore{currentRev: 10}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 3, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	require.Contains(t, hub.unsynced, w.id)

	hub.mu.Lock()
	hub.unsafePromote(w.id)
	hub.mu.Unlock()

	require.NotContains(t, hub.unsynced, w.id)
	require.Contains(t, hub.synced, w.id)
}

func Test_WatchHub_Drop_ClosesAndRemoves(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	hub.mu.Lock()
	hub.unsafeDropFromGroup(hub.synced, w.id)
	hub.mu.Unlock()

	require.NotContains(t, hub.synced, w.id)
	require.Error(t, w.ctx.Err(), "context should be cancelled")

	_, ok := <-w.c
	require.False(t, ok, "channel should be closed")
}

func Test_WatchHub_WatchIDCollision(t *testing.T) {
	store := &mockStore{}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{WatchID: 1, StartRevision: 5, Key: []byte("foo")}
	_, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	_, err = hub.NewWatcher(t.Context(), req)
	require.ErrorIs(t, err, ErrWatcherIDConflict)
}

func Test_WatchHub_OnCommit_PrefixWatcher(t *testing.T) {
	store := &mockStore{currentRev: 5}
	hub := newMockHub(store)

	req := api.WatchCreateRequest{StartRevision: 5, Key: []byte("app"), Prefix: true}

	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	hub.OnCommit([]*kv.Entry{
		putEntry("app", "v1", 6),         // exact
		putEntry("apple", "v2", 7),       // prefix
		putEntry("application", "v3", 8), // prefix
		putEntry("apq", "v4", 9),         // excluded
		putEntry("banana", "v5", 10),     // excluded
	})

	got := drainEvents(w.c)
	require.Len(t, got, 3)
	require.Equal(t, "app", string(got[0].Entry.Key))
	require.Equal(t, "apple", string(got[1].Entry.Key))
	require.Equal(t, "application", string(got[2].Entry.Key))
}
