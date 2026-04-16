package watch

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/types/api"
	"github.com/stretchr/testify/require"
)

func newTestStream(t *testing.T, storeRev int64) (*stream, *mockStore, *WatchHub) {
	t.Helper()
	store := &mockStore{currentRev: storeRev}
	hub := newMockHub(store)
	s := NewStream(t.Context(), slog.Default(), hub).(*stream)
	t.Cleanup(s.Close)
	return s, store, hub
}

func watchReq(key string, startRev int64) api.WatchCreateRequest {
	return api.WatchCreateRequest{
		Key:           []byte(key),
		StartRevision: startRev,
	}
}

func watchRangeReq(key, end string, startRev int64) api.WatchCreateRequest {
	return api.WatchCreateRequest{
		Key:           []byte(key),
		End:           []byte(end),
		StartRevision: startRev,
	}
}

func collectStreamEvents(t *testing.T, s *stream, n int) (out []StreamEvent) {
	t.Helper()
	for range n {
		select {
		case ev, ok := <-s.Events():
			if !ok {
				t.Fatalf("output channel closed after %d events, expected %d", len(out), n)
			}
			out = append(out, ev)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out after %d events, expected %d", len(out), n)
		}
	}
	return out
}

func expectNoStreamEvents(t *testing.T, s *stream) {
	t.Helper()
	select {
	case ev, ok := <-s.Events():
		if ok {
			t.Fatalf("unexpected stream event: %+v", ev)
		}
	case <-time.After(50 * time.Millisecond):
		// ok
	}
}

func Test_Stream_Watch_ReturnsWatchID(t *testing.T) {
	ws, _, _ := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)
	require.NotZero(t, res.WatchID)
}

func Test_Stream_Watch_RegistersWatcherOnHub(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	require.Contains(t, hub.TestSyncedCloned(), res.WatchID, "expected startRev == storeRev")
}

func Test_Stream_Watch_MultipleDifferentKeys(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res1 := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res1.Error)

	res2 := ws.Watch(t.Context(), watchReq("bar", 5))
	require.NoError(t, res2.Error)

	require.NotEqual(t, res1.WatchID, res2)
	require.Contains(t, hub.TestSyncedCloned(), res1.WatchID)
	require.Contains(t, hub.TestSyncedCloned(), res2.WatchID)
}

func Test_Stream_Watch_UnsyncedPlacement(t *testing.T) {
	ws, _, hub := newTestStream(t, 10)

	res := ws.Watch(t.Context(), watchReq("foo", 3))
	require.NoError(t, res.Error)

	require.Contains(t, hub.TestUnsyncedCloned(), res.WatchID)
}

func Test_Stream_Watch_EventsFlowToOutput(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	hub.OnCommit([]*kv.Entry{putEntry("foo", "bar", 6)})

	got := collectStreamEvents(t, ws, 1)
	require.Equal(t, res.WatchID, got[0].Wid)
	require.Equal(t, kv.EventPut, got[0].Event.Kind)

	ev := got[0].Event
	require.Equal(t, "foo", string(ev.Entry.Key))
	require.Equal(t, "bar", string(ev.Entry.Value))
}

func Test_Stream_Watch_DeleteEventFlowsToOutput(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	hub.OnCommit([]*kv.Entry{testTombstoneEntry("foo", 6)})

	got := collectStreamEvents(t, ws, 1)
	require.Equal(t, res.WatchID, got[0].Wid)
	require.Equal(t, kv.EventDelete, got[0].Event.Kind)
}

func Test_Stream_Watch_MultipleEventsInOneCommit(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	hub.OnCommit([]*kv.Entry{
		putEntry("foo", "v1", 6),
		putEntry("foo", "v2", 7),
		putEntry("foo", "v3", 8),
	})

	got := collectStreamEvents(t, ws, 3)
	require.Len(t, got, 3)

	// events with the same rev as the last rev
	// all skipped since revs should be monotonic
	hub.OnCommit([]*kv.Entry{
		putEntry("foo", "v4", 8),
		putEntry("foo", "v4", 8),
		putEntry("foo", "v4", 8),
	})

	expectNoStreamEvents(t, ws)
}

func Test_Stream_FanIn_MultipleWatchersOnSingleOutput(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res1 := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res1.Error)

	res2 := ws.Watch(t.Context(), watchReq("bar", 5))
	require.NoError(t, res2.Error)

	hub.OnCommit([]*kv.Entry{
		putEntry("foo", "fv", 6),
		putEntry("bar", "bv", 6),
	})

	got := collectStreamEvents(t, ws, 2)

	rs := map[int64]bool{got[0].Wid: true, got[1].Wid: true}
	require.True(t, rs[res1.WatchID], "should see event from watcher 1")
	require.True(t, rs[res2.WatchID], "should see event from watcher 2")
}

func Test_Stream_FanIn_NonMatchingKeyFilteredOut(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	hub.OnCommit([]*kv.Entry{putEntry("bar", "v", 6)})

	expectNoStreamEvents(t, ws)
}

func Test_Stream_Cancel_RemovesWatcher(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)
	require.Contains(t, hub.TestSyncedCloned(), res.WatchID)

	ws.Cancel(api.WatchCancelRequest{WatchID: res.WatchID})

	time.Sleep(50 * time.Millisecond)

	require.NotContains(t, hub.TestSyncedCloned(), res.WatchID, "cancelled watcher should be removed from hub")
	require.NotContains(t, hub.TestUnsyncedCloned(), res.WatchID)
}

func Test_Stream_Cancel_StopsEventDelivery(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	// deliver one event first to confirm it works
	hub.OnCommit([]*kv.Entry{putEntry("foo", "v1", 6)})
	got := collectStreamEvents(t, ws, 1)
	require.Equal(t, res.WatchID, got[0].Wid)

	ws.Cancel(api.WatchCancelRequest{WatchID: res.WatchID})
	time.Sleep(50 * time.Millisecond)

	hub.OnCommit([]*kv.Entry{putEntry("foo", "v2", 7)})
	expectNoStreamEvents(t, ws)
}

func Test_Stream_Cancel_NonExistentWatchIDIsNoop(t *testing.T) {
	ws, _, _ := newTestStream(t, 5)

	require.NotPanics(t, func() {
		ws.Cancel(api.WatchCancelRequest{WatchID: 999999})
	})
}

func Test_Stream_Cancel_OnlyAffectsTargetedWatcher(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res1 := ws.Watch(t.Context(), watchReq("foo", 5))
	res2 := ws.Watch(t.Context(), watchReq("bar", 5))

	// cancel only watcher 1
	ws.Cancel(api.WatchCancelRequest{WatchID: res1.WatchID})
	time.Sleep(50 * time.Millisecond)

	// watcher 2 should still receive events
	hub.OnCommit([]*kv.Entry{putEntry("bar", "v", 6)})

	got := collectStreamEvents(t, ws, 1)
	require.Equal(t, res2.WatchID, got[0].Wid)
}

func Test_Stream_Close_ClosesOutputChannel(t *testing.T) {
	ws, _, _ := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	ws.Close()

	_, ok := <-ws.Events()
	require.False(t, ok, "output channel should be closed after Close()")
}

func Test_Stream_Close_DropsAllStreamWatchers(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res1 := ws.Watch(t.Context(), watchReq("foo", 5))
	res2 := ws.Watch(t.Context(), watchReq("bar", 5))

	ws.Close()
	time.Sleep(50 * time.Millisecond)

	require.NotContains(t, hub.TestSyncedCloned(), res1.WatchID)
	require.NotContains(t, hub.TestSyncedCloned(), res2.WatchID)
}

func Test_Stream_Close_NoEventsAfterClose(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	ws.Close()

	require.NotPanics(t, func() {
		hub.OnCommit([]*kv.Entry{putEntry("foo", "v", 6)})
		_, ok := <-ws.Events()
		require.False(t, ok, "output channel should be closed after Close()")
	})
}

func Test_Stream_Close_Idempotent(t *testing.T) {
	ws, _, _ := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res.Error)

	ws.Close()

	require.NotPanics(t, func() {
		ws.Close()
	})
}

func Test_Stream_Watch_ContextCancelStopsGoroutine(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	ctx, cancel := context.WithCancel(t.Context())
	res := ws.Watch(ctx, watchReq("foo", 5))
	require.NoError(t, res.Error)

	cancel()
	time.Sleep(100 * time.Millisecond)

	require.NotContains(t, hub.TestSyncedCloned(), res.WatchID, "cancelled watcher should be removed")
	require.NotContains(t, hub.TestUnsyncedCloned(), res.WatchID)
}

func Test_Stream_Watch_ContextCancelDoesNotAffectOtherWatchers(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	ctx1, cancel1 := context.WithCancel(t.Context())
	res1 := ws.Watch(ctx1, watchReq("foo", 5))

	res2 := ws.Watch(t.Context(), watchReq("bar", 5))

	cancel1()
	time.Sleep(100 * time.Millisecond)

	require.NotContains(t, hub.TestSyncedCloned(), res1.WatchID, "cancelled watcher should be gone")
	require.Contains(t, hub.TestSyncedCloned(), res2.WatchID, "other watcher should survive")

	hub.OnCommit([]*kv.Entry{putEntry("bar", "v", 6)})
	got := collectStreamEvents(t, ws, 1)
	require.Equal(t, res2.WatchID, got[0].Wid, "watcher 2 should still receive events")
}

func Test_Stream_Watch_RangeWatch(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res := ws.Watch(t.Context(), watchRangeReq("a", "d", 5))
	require.NoError(t, res.Error)

	hub.OnCommit([]*kv.Entry{
		putEntry("a", "va", 6),
		putEntry("c", "vc", 7),
		putEntry("d", "vd", 8), // excluded
		putEntry("z", "vz", 9), // excluded
	})

	got := collectStreamEvents(t, ws, 2)
	require.Len(t, got, 2)
	require.Equal(t, res.WatchID, got[0].Wid)
	require.Equal(t, res.WatchID, got[1].Wid)

	event1 := got[0].Event
	require.Equal(t, "a", string(event1.Entry.Key))
	require.Equal(t, "va", string(event1.Entry.Value))

	event2 := got[1].Event
	require.Equal(t, "c", string(event2.Entry.Key))
	require.Equal(t, "vc", string(event2.Entry.Value))
}

func Test_Stream_ToStreamEvent_PutMapping(t *testing.T) {
	se := putEvent(42, kv.Event{Kind: kv.EventPut, Entry: putEntry("foo", "bar", 1)})

	require.Equal(t, int64(42), se.Wid)
	require.Equal(t, kv.EventPut, se.Event.Kind)

	event := se.Event
	require.Equal(t, "bar", string(event.Entry.Value))
}

func Test_Stream_ToStreamEvent_DeleteMapping(t *testing.T) {
	se := deleteEvent(42, kv.Event{Kind: kv.EventDelete, Entry: testTombstoneEntry("foo", 3)})

	require.Equal(t, int64(42), se.Wid)
	require.Equal(t, kv.EventDelete, se.Event.Kind)
}

func Test_Stream_RapidWatchCancel(t *testing.T) {
	ws, _, _ := newTestStream(t, 5)

	for range 50 {
		res := ws.Watch(t.Context(), watchReq("foo", 5))
		require.NoError(t, res.Error)
		ws.Cancel(api.WatchCancelRequest{WatchID: res.WatchID})
	}

	time.Sleep(100 * time.Millisecond)
}

func Test_Stream_ReWatchAfterCancel(t *testing.T) {
	ws, _, hub := newTestStream(t, 5)

	res1 := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res1.Error)
	ws.Cancel(api.WatchCancelRequest{WatchID: res1.WatchID})
	time.Sleep(50 * time.Millisecond)

	res2 := ws.Watch(t.Context(), watchReq("foo", 5))
	require.NoError(t, res2.Error)

	hub.OnCommit([]*kv.Entry{putEntry("foo", "new", 6)})

	got := collectStreamEvents(t, ws, 1)
	require.Equal(t, res2.WatchID, got[0].Wid)

	ev := got[0].Event
	require.Equal(t, "new", string(ev.Entry.Value))
}
