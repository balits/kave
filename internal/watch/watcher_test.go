package watch

import (
	"context"
	"fmt"
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

const testWatcherChannelSize = 4

func newTestWatcher(t *testing.T, key, end []byte, startRev int64, f *kv.EventFilter, ctx context.Context) *watcher {
	t.Helper()

	var derivedCtx context.Context
	var cancel context.CancelFunc

	if ctx == nil {
		derivedCtx, cancel = context.WithCancel(t.Context())
	} else {
		derivedCtx, cancel = context.WithCancel(ctx)
	}

	w := &watcher{
		id:         util.NextNonNullID(),
		startRev:   startRev,
		currentRev: startRev,
		keyStart:   key,
		keyEnd:     end,
		ctx:        derivedCtx,
		cancel:     cancel,
		filter:     f,
		c:          make(chan kv.Event, testWatcherChannelSize),
	}
	return w
}

func Test_Watcher_Send_EventsMatch(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, nil, nil)

	put := testPutEvent(key, "value", 5)
	sent, err := w.send(put)
	require.True(t, sent)
	require.NoError(t, err)

	got := drainEvents(w.c)
	require.Len(t, got, 1)
	require.Equal(t, kv.EventPut, got[0].Kind)
	require.Equal(t, key, string(got[0].Entry.Key))
	require.Equal(t, int64(5), got[0].Entry.ModRev)
}

func Test_Watcher_Send_SkipsNonMatchingKey(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, nil, nil)

	ev := testPutEvent("bar", "val", 5)
	sent, err := w.send(ev)
	require.False(t, sent)
	require.NoError(t, err)

	got := drainEvents(w.c)
	require.Empty(t, got, "non-matching key should not be delivered")
}

func Test_Watcher_Send_Idempotent_SkipsOldRevision(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 10, nil, nil)

	// 8 < 10, should skip
	ev := testPutEvent("foo", "old", 8)
	sent, err := w.send(ev)
	require.False(t, sent)
	require.NoError(t, err)

	got := drainEvents(w.c)
	require.Empty(t, got, "event at rev <= currentRev should be skipped")
}

func Test_Watcher_Send_Idempotent_SkipsEqualRevision(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 5, nil, nil)

	// 5 == 5, no new event, should skip
	ev := testPutEvent("foo", "dup", 5)
	sent, err := w.send(ev)
	require.False(t, sent)
	require.NoError(t, err)

	got := drainEvents(w.c)
	require.Empty(t, got, "event at rev == currentRev should be skipped")
}

func Test_Watcher_Send_FilterPut(t *testing.T) {
	f := kv.FilterNoPut
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, &f, nil)

	sent, err := w.send(testPutEvent("foo", "v", 1))
	require.False(t, sent)
	require.NoError(t, err)
	require.Empty(t, drainEvents(w.c))

	sent, err = w.send(testDeleteEvent("foo", 2))
	require.True(t, sent)
	require.NoError(t, err)
	got := drainEvents(w.c)
	require.Len(t, got, 1)
	require.Equal(t, kv.EventDelete, got[0].Kind)
}

func Test_Watcher_Send_FilterNoDelete(t *testing.T) {
	f := kv.FilterNoDelete
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, &f, nil)

	sent, err := w.send(testDeleteEvent("foo", 1))
	require.False(t, sent)
	require.NoError(t, err)
	require.Empty(t, drainEvents(w.c))

	sent, err = w.send(testPutEvent("foo", "v", 2))
	require.True(t, sent)
	require.NoError(t, err)
	got := drainEvents(w.c)
	require.Len(t, got, 1)
	require.Equal(t, kv.EventPut, got[0].Kind)
}

func Test_Watcher_Send_OverloadsWhenFull(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)

	for i := range testWatcherChannelSize {
		sent, err := w.send(testPutEvent("foo", fmt.Sprintf("v%d", i+1), int64(i+1)))
		require.True(t, sent)
		require.NoError(t, err)
	}

	sent, err := w.send(testPutEvent("foo", "overload", testWatcherChannelSize+1))
	require.False(t, sent)
	require.ErrorIs(t, err, ErrWatcherOverloaded)
}

func Test_Watcher_Send_ReturnsErrorOnCancelledCtx(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)

	w.cancel()

	sent, err := w.send(testPutEvent("foo", "v", 5))
	require.False(t, sent)
	require.ErrorIs(t, err, context.Canceled)
}

func Test_Watcher_SendAll_AdvancesCurrentRev(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)

	events := []kv.Event{
		testPutEvent("foo", "a", 3),
		testPutEvent("foo", "b", 5),
		testPutEvent("foo", "c", 8),
	}
	err := w.sendAll(events)
	require.NoError(t, err)
	require.Equal(t, int64(8), w.currentRev, "currentRev should advance to last event's ModRev")

	got := drainEvents(w.c)
	require.Len(t, got, 3)
}

func Test_Watcher_SendAll_StopsOnError(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)
	close(w.c)
	w.c = make(chan kv.Event, 1)

	events := []kv.Event{
		testPutEvent("foo", "a", 3),
		testPutEvent("foo", "b", 5),
		testPutEvent("foo", "c", 8),
	}
	err := w.sendAll(events)
	require.ErrorIs(t, err, ErrWatcherOverloaded)
	require.Equal(t, int64(3), w.currentRev)
}

func Test_Watcher_SendAll_DoesNotRegressCurrentRev(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 10, nil, nil)

	events := []kv.Event{
		testPutEvent("foo", "old1", 7),
		testPutEvent("foo", "old2", 9),
	}

	err := w.sendAll(events)
	require.NoError(t, err)
	require.Equal(t, int64(10), w.currentRev, "currentRev must not regress")
	require.Empty(t, drainEvents(w.c), "no events should be delivered")
}

func Test_Watcher_SendAll_MixedOldAndNewEvents(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 5, nil, nil)

	events := []kv.Event{
		testPutEvent("foo", "old", 3),  // below currentRev, skipped
		testPutEvent("foo", "dup", 5),  // equals currentRev, skipped
		testPutEvent("foo", "new1", 7), // above currentRev, delivered
		testPutEvent("foo", "new2", 9), // above currentRev, delivered
	}

	err := w.sendAll(events)
	require.NoError(t, err)
	require.Equal(t, int64(9), w.currentRev)

	got := drainEvents(w.c)
	require.Len(t, got, 2, "only events above currentRev should be delivered")
	require.Equal(t, "new1", string(got[0].Entry.Value))
	require.Equal(t, "new2", string(got[1].Entry.Value))
}

func Test_Watcher_SendAll_NilKeyStart_CatchesAllEvent(t *testing.T) {
	w := newTestWatcher(t, nil, nil, 0, nil, nil)

	events := []kv.Event{
		testPutEvent("foo", "v", 1),
		testPutEvent("bar", "v", 2),
	}

	err := w.sendAll(events)
	require.NoError(t, err)
	require.Equal(t, int64(2), w.currentRev)
	require.Len(t, drainEvents(w.c), 2)
}

func Test_Watcher_Matches_PointWatch(t *testing.T) {
	w := &watcher{keyStart: []byte("foo")}

	require.True(t, w.matches(testPutEvent("foo", "v", 1)))
	require.False(t, w.matches(testPutEvent("bar", "v", 1)))
	require.False(t, w.matches(testPutEvent("foobar", "v", 1)))
	require.False(t, w.matches(testPutEvent("fo", "v", 1)))
}

func Test_Watcher_Matches_RangeWatch(t *testing.T) {
	// [a, d)
	w := &watcher{keyStart: []byte("a"), keyEnd: []byte("d")}

	require.True(t, w.matches(testPutEvent("a", "v", 1)))
	require.True(t, w.matches(testPutEvent("b", "v", 1)))
	require.True(t, w.matches(testPutEvent("c", "v", 1)))
	require.False(t, w.matches(testPutEvent("d", "v", 1)))
	require.False(t, w.matches(testPutEvent("e", "v", 1)))
	require.False(t, w.matches(testPutEvent("Z", "v", 1)))
}

func Test_Watcher_Matches_PrefixRangeWatch(t *testing.T) {
	key := []byte("app")
	w := &watcher{keyStart: key, keyEnd: kv.PrefixEnd(key)}
	t.Log(w)

	require.True(t, w.matches(testPutEvent("app", "v1", 6)))
	require.True(t, w.matches(testPutEvent("apple", "v2", 7)))
	require.True(t, w.matches(testPutEvent("application", "v3", 8)))
	require.False(t, w.matches(testPutEvent("apq", "v4", 9)))
	require.False(t, w.matches(testPutEvent("banana", "v5", 10)))
}

func Test_Watcher_Close_CancelsContextAndClosesChannel(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)
	w.close()

	require.Error(t, w.ctx.Err(), "context should be cancelled after close")

	_, ok := <-w.c
	require.False(t, ok, "channel should be closed after close")
}
