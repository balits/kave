package watch

import (
	"context"
	"fmt"
	"testing"

	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

const testWatcherChannelSize = 4

func newTestWatcher(t *testing.T, key, end []byte, startRev int64, f *Filter, ctx context.Context) *watcher {
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
		c:          make(chan Event, testWatcherChannelSize),
	}
	return w
}

// The tests you should write, in this order, would start with the watcher unit in isolation.
// Create a watcher directly, call send and sendAll with known events,
// verify the channel receives exactly the right events,
// verify currentRev advances correctly,
// verify filtered events are skipped but currentRev still advances,
// verify the idempotent check works (send the same event twice, assert it's only delivered once), and
// verify errWatcherOverloaded fires when the channel is full.

func Test_Watcher_Send_EventsMatch(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, nil, nil)

	put := putEvent(key, "value", 5)
	require.NoError(t, w.send(put))

	got := drainEvents(w.c)
	require.Len(t, got, 1)
	require.Equal(t, EventPut, got[0].Kind)
	require.Equal(t, key, string(got[0].Entry.Key))
	require.Equal(t, int64(5), got[0].Entry.ModRev)
}

func Test_Watcher_Send_SkipsNonMatchingKey(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, nil, nil)

	ev := putEvent("bar", "val", 5)
	require.NoError(t, w.send(ev))

	got := drainEvents(w.c)
	require.Empty(t, got, "non-matching key should not be delivered")
}

func Test_Watcher_Send_Idempotent_SkipsOldRevision(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 10, nil, nil)

	// 8 < 10, should skip
	ev := putEvent("foo", "old", 8)
	err := w.send(ev)
	require.NoError(t, err)

	got := drainEvents(w.c)
	require.Empty(t, got, "event at rev <= currentRev should be skipped")
}

func Test_Watcher_Send_Idempotent_SkipsEqualRevision(t *testing.T) {
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 5, nil, nil)

	// 5 == 5, no new event, should skip
	ev := putEvent("foo", "dup", 5)
	err := w.send(ev)
	require.NoError(t, err)

	got := drainEvents(w.c)
	require.Empty(t, got, "event at rev == currentRev should be skipped")
}

func Test_Watcher_Send_FilterPut(t *testing.T) {
	f := FilterNoPut
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, &f, nil)

	require.NoError(t, w.send(putEvent("foo", "v", 1)))
	require.Empty(t, drainEvents(w.c))

	require.NoError(t, w.send(deleteEvent("foo", 2)))
	got := drainEvents(w.c)
	require.Len(t, got, 1)
	require.Equal(t, EventDelete, got[0].Kind)
}

func Test_Watcher_Send_FilterNoDelete(t *testing.T) {
	f := FilterNoDelete
	key := "foo"
	w := newTestWatcher(t, []byte(key), nil, 0, &f, nil)

	require.NoError(t, w.send(deleteEvent("foo", 1)))
	require.Empty(t, drainEvents(w.c))

	require.NoError(t, w.send(putEvent("foo", "v", 2)))
	got := drainEvents(w.c)
	require.Len(t, got, 1)
	require.Equal(t, EventPut, got[0].Kind)
}

func Test_Watcher_Send_OverloadsWhenFull(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)

	for i := range testWatcherChannelSize {
		require.NoError(t, w.send(putEvent("foo", fmt.Sprintf("v%d", i+1), int64(i+1))))
	}

	err := w.send(putEvent("foo", "overload", testWatcherChannelSize+1))
	require.ErrorIs(t, err, errWatcherOverloaded)
}

func Test_Watcher_Send_ReturnsErrorOnCancelledCtx(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)

	w.cancel()

	err := w.send(putEvent("foo", "v", 5))
	require.ErrorIs(t, err, context.Canceled)
}

func Test_Watcher_SendAll_AdvancesCurrentRev(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)

	events := []Event{
		putEvent("foo", "a", 3),
		putEvent("foo", "b", 5),
		putEvent("foo", "c", 8),
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
	w.c = make(chan Event, 1)

	events := []Event{
		putEvent("foo", "a", 3),
		putEvent("foo", "b", 5),
		putEvent("foo", "c", 8),
	}
	err := w.sendAll(events)
	require.ErrorIs(t, err, errWatcherOverloaded)
	require.Equal(t, int64(3), w.currentRev)
}

func Test_Watcher_SendAll_DoesNotRegressCurrentRev(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 10, nil, nil)

	events := []Event{
		putEvent("foo", "old1", 7),
		putEvent("foo", "old2", 9),
	}

	err := w.sendAll(events)
	require.NoError(t, err)
	require.Equal(t, int64(10), w.currentRev, "currentRev must not regress")
	require.Empty(t, drainEvents(w.c), "no events should be delivered")
}

func Test_Watcher_SendAll_MixedOldAndNewEvents(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 5, nil, nil)

	events := []Event{
		putEvent("foo", "old", 3),  // below currentRev, skipped
		putEvent("foo", "dup", 5),  // equals currentRev, skipped
		putEvent("foo", "new1", 7), // above currentRev, delivered
		putEvent("foo", "new2", 9), // above currentRev, delivered
	}

	err := w.sendAll(events)
	require.NoError(t, err)
	require.Equal(t, int64(9), w.currentRev)

	got := drainEvents(w.c)
	require.Len(t, got, 2, "only events above currentRev should be delivered")
	require.Equal(t, "new1", string(got[0].Entry.Value))
	require.Equal(t, "new2", string(got[1].Entry.Value))
}

func Test_Watcher_Matches_PointWatch(t *testing.T) {
	w := &watcher{keyStart: []byte("foo")}

	require.True(t, w.matches(putEvent("foo", "v", 1)))
	require.False(t, w.matches(putEvent("bar", "v", 1)))
	require.False(t, w.matches(putEvent("foobar", "v", 1)))
	require.False(t, w.matches(putEvent("fo", "v", 1)))
}

func Test_Watcher_Matches_RangeWatch(t *testing.T) {
	// [a, d)
	w := &watcher{keyStart: []byte("a"), keyEnd: []byte("d")}

	require.True(t, w.matches(putEvent("a", "v", 1)))  // inclusive start
	require.True(t, w.matches(putEvent("b", "v", 1)))  // within range
	require.True(t, w.matches(putEvent("c", "v", 1)))  // within range
	require.False(t, w.matches(putEvent("d", "v", 1))) // exclusive end
	require.False(t, w.matches(putEvent("e", "v", 1))) // beyond range
	require.False(t, w.matches(putEvent("Z", "v", 1))) // before range (uppercase)
}

func Test_Watcher_Close_CancelsContextAndClosesChannel(t *testing.T) {
	w := newTestWatcher(t, []byte("foo"), nil, 0, nil, nil)
	w.close()

	require.Error(t, w.ctx.Err(), "context should be cancelled after close")

	_, ok := <-w.c
	require.False(t, ok, "channel should be closed after close")
}
