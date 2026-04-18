package watch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types/api"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func newTestLoop(t *testing.T) (*UnsyncedLoop, *WatchHub, *mvcc.KvStore) {
	t.Helper()
	logger := testLogger()
	reg := metrics.InitTestPrometheus()
	b := backend.New(reg, logger, storage.Options{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	t.Cleanup(func() { b.Close() })
	index := kv.NewTreeIndex(logger)
	store := mvcc.NewKvStoreWithIndex(reg, logger, b, index)
	hub := NewWatchHub(reg, logger, store)
	loop := NewUnsyncedLoop(logger, hub, index, b.ReadTx(), store)
	loop.ctx, loop.cancel = context.WithCancel(t.Context())
	return loop, hub, store
}

// each call creates a new writer, so each call bumps the store revision by 1.
func mustPut(t *testing.T, store *mvcc.KvStore, key, value string) {
	t.Helper()
	w := store.NewWriter()
	err := w.Put([]byte(key), []byte(value), 0)
	require.NoError(t, err)
	require.NoError(t, w.End())
}

// tombstone a key through a real writer transaction.
func mustDelete(t *testing.T, store *mvcc.KvStore, key string) {
	t.Helper()
	w := store.NewWriter()
	err := w.DeleteKey([]byte(key))
	require.NoError(t, err)
	require.NoError(t, w.End())
}

func Test_UnsyncedLoop_CatchesUpAndPromotes(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	// write "foo" at revisions 1-5
	for i := 1; i <= 5; i++ {
		mustPut(t, store, "foo", fmt.Sprintf("v%d", i))
	}

	// watcher starts at rev 1, so it needs to catch up on revisions 2-5
	req := api.WatchCreateRequest{StartRevision: 1, Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	require.Contains(t, hub.unsynced, w.id)

	loop.tick()

	require.NotContains(t, hub.unsynced, w.id, "should be removed from unsynced")
	require.Contains(t, hub.synced, w.id, "should be promoted to synced")

	got := drainEvents(w.c)
	require.Len(t, got, 4, "should receive events for revisions 2-5")
	require.Equal(t, kv.EventPut, got[0].Kind)
	require.Equal(t, int64(5), w.currentRev)
}

func Test_UnsyncedLoop_PartialCatchUpOnKey_StillPromotes(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "v1")
	mustPut(t, store, "foo", "v2")
	mustPut(t, store, "foo", "v3")

	for i := 4; i <= 10; i++ {
		mustPut(t, store, "bar", fmt.Sprintf("v%d", i))
	}

	// store is at rev 10 but foo was last written at rev 3
	rev, _ := store.Revisions()
	require.Equal(t, int64(10), rev.Main)

	// watcher on foo starting at rev 0
	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	loop.tick()

	// watcher should be promoted even though store is at rev 10,
	require.Contains(t, hub.synced, w.id, "watcher should promote based on key revision, not store revision")
	require.NotContains(t, hub.unsynced, w.id)
	require.Equal(t, int64(3), w.currentRev)

	got := drainEvents(w.c)
	require.Len(t, got, 3)
}

func Test_UnsyncedLoop_CompactionErrorDropsWatcher(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "v1")
	mustPut(t, store, "foo", "v2")

	ch, err := store.Compact(2)
	require.NoError(t, err)
	<-ch

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	loop.tick()

	require.NotContains(t, hub.unsynced, w.id, "watcher should be removed on error kv.ErrCompacted")
	require.Empty(t, drainEvents(w.c))
}

func Test_UnsyncedLoop_DropsWatcherAfterMaxFailures(t *testing.T) {
	t.Skip("skipped: havent fought of a way to simulate max failures easily")
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "v1")
	mustPut(t, store, "foo", "v2")

	ch, err := store.Compact(2)
	require.NoError(t, err)
	<-ch

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	for range unsynchedFailureLimit {
		loop.tick()
	}
	require.Contains(t, hub.unsynced, w.id, "should survive up to the limit")
	require.Equal(t, unsynchedFailureLimit, loop.unsyncedFailures[w.id])

	// one more tick triggers the drop
	loop.tick()

	require.NotContains(t, hub.unsynced, w.id, "should be dropped after exceeding limit")
	require.NotContains(t, loop.unsyncedFailures, w.id, "failure counter should be cleaned")
	require.Error(t, w.ctx.Err())
	_, ok := <-w.c
	require.False(t, ok)
}

func Test_UnsyncedLoop_DropsWatcherOnContextCancel(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "v1")
	mustPut(t, store, "foo", "v2")

	ctx, cancel := context.WithCancel(t.Context())
	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(ctx, req)
	require.NoError(t, err)

	cancel()

	loop.tick()

	require.NotContains(t, hub.unsynced, w.id, "cancelled watcher should be dropped")
}

func Test_UnsyncedLoop_OverloadIncrementsFailureButKeepsWatcher(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	for i := 1; i <= watcherChannelBufferSize*2; i++ {
		mustPut(t, store, "foo", fmt.Sprintf("v%d", i))
	}

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)
	loop.tick()

	require.Contains(t, hub.unsynced, w.id, "overloaded watcher should NOT be dropped on first failure")
	require.Equal(t, 1, loop.unsyncedFailures[w.id])
}

func Test_UnsyncedLoop_ClearsFailureCounterOnPromotion(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "v1")

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	// seed a prior failure
	loop.unsyncedFailures[w.id] = 2

	loop.tick()

	require.Contains(t, hub.synced, w.id)
	require.NotContains(t, loop.unsyncedFailures, w.id, "failure counter should be deleted on promotion")
}

func Test_UnsyncedLoop_Integration_AfterPromotion_OnCommitDelivers(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "historical")

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	loop.tick()
	require.Contains(t, hub.synced, w.id)

	// drain catch-up events
	catchUp := drainEvents(w.c)
	require.Len(t, catchUp, 1)
	require.Equal(t, "historical", string(catchUp[0].Entry.Value))

	// write new data through the store
	// In real usage, Engine.ApplyWrite calls w.End() then hub.OnCommit(changes).
	sw := store.NewWriter()
	require.NoError(t, sw.Put([]byte("foo"), []byte("live"), 0))
	require.NoError(t, sw.End())
	_, changes := sw.UnsafeExpectedChanges()
	hub.OnCommit(changes)

	live := expectEvents(t, w.c, 1)
	require.Equal(t, "live", string(live[0].Entry.Value))
}

func Test_UnsyncedLoop_Integration_NoDoubleDeliveryOnPromotion(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "v1") // rev 1
	mustPut(t, store, "foo", "v2") // rev 2

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	loop.tick()
	require.Contains(t, hub.synced, w.id)
	require.Equal(t, int64(2), w.currentRev)

	// simulate the race: OnCommit fires with the same rev 2 data
	// (as if the Engine callback arrived right after promotion)
	hub.OnCommit([]*kv.Entry{
		{Key: []byte("foo"), Value: []byte("v2-dup"), ModRev: 2, CreateRev: 1, Version: 2},
	})

	all := drainEvents(w.c)
	require.Len(t, all, 2, "idempotent check should prevent double delivery")
	require.Equal(t, "v1", string(all[0].Entry.Value))
	require.Equal(t, "v2", string(all[1].Entry.Value))
}

func Test_UnsyncedLoop_EmptyGroupNoOp(t *testing.T) {
	loop, _, _ := newTestLoop(t)
	require.NotPanics(t, func() { loop.tick() })
}

func Test_UnsyncedLoop_ProcessesMultipleWatchers(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "a", "va") // rev 1
	mustPut(t, store, "b", "vb") // rev 2

	req1 := api.WatchCreateRequest{Key: []byte("a")}
	w1, err := hub.NewWatcher(t.Context(), req1)
	require.NoError(t, err)

	req2 := api.WatchCreateRequest{Key: []byte("b")}
	w2, err := hub.NewWatcher(t.Context(), req2)
	require.NoError(t, err)

	loop.tick()

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
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "foo", "alive")
	mustDelete(t, store, "foo")

	req := api.WatchCreateRequest{Key: []byte("foo")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	loop.tick()
	time.Sleep(200 * time.Millisecond)
	got := drainEvents(w.c)
	require.Len(t, got, 2)
	require.Equal(t, kv.EventPut, got[0].Kind)
	require.Equal(t, kv.EventDelete, got[1].Kind)
}

func Test_UnsyncedLoop_RangeWatch_CatchesUpMultipleKeys(t *testing.T) {
	loop, hub, store := newTestLoop(t)

	mustPut(t, store, "a", "va") // rev 1
	mustPut(t, store, "b", "vb") // rev 2
	mustPut(t, store, "c", "vc") // rev 3
	mustPut(t, store, "d", "vd") // rev 4

	req := api.WatchCreateRequest{Key: []byte("a"), End: []byte("d")}
	w, err := hub.NewWatcher(t.Context(), req)
	require.NoError(t, err)

	loop.tick()

	got := drainEvents(w.c)
	require.Len(t, got, 3)

	keys := make([]string, len(got))
	for i, ev := range got {
		keys[i] = string(ev.Entry.Key)
	}
	require.Contains(t, keys, "a")
	require.Contains(t, keys, "b")
	require.Contains(t, keys, "c")
}
