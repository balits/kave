package watch

import (
	"bytes"
	"context"
	"errors"

	"github.com/balits/kave/internal/types"
)

const (
	watcherChannelBufferSize = 128
)

// errWatcherOverloaded is returned when a watcher's channel is full and it cannot receive new events.
var errWatcherOverloaded = errors.New("watcher channel is overloaded")

type EventKind string

const (
	EventPut    EventKind = "PUT"
	EventDelete EventKind = "DELETE"
)

type Filter string

const (
	FilterNoPut    Filter = "NO_PUT"
	FilterNoDelete Filter = "NO_DELETE"
)

type Event struct {
	Kind  EventKind
	Entry types.KvEntry
	// TODO(1): not used yet, but will add later:
	// ([]changes kv.Entry) -> []{Entry, PrevEntry} but idk if we should add extra reads for each write:(
	PrevEntry *types.KvEntry
}

type watcher struct {
	id         int64
	keyStart   []byte
	keyEnd     []byte
	startRev   int64
	currentRev int64
	ctx        context.Context
	cancel     context.CancelFunc
	filter     *Filter // either NO_PUT or NO_DELETE, or empty for no filter
	c          chan Event
}

func (w *watcher) close() {
	w.cancel()
	close(w.c)
}

// send is idempotent, because of the race between OnCommit and UnsynchedWatcherLoop, protecting against double delivery.
// In practice, an event with an earlier rev compared to the watchers current rev is skipped.
func (w *watcher) send(e Event) error {
	if w.ctx.Err() != nil {
		return w.ctx.Err()
	}

	if e.Entry.ModRev <= w.currentRev {
		return nil
	}

	if !w.matches(e) {
		return nil
	}

	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	case w.c <- e:
		return nil
	default:
		return errWatcherOverloaded
	}
}

// sendAll sends every event it gets to the watcher in a non blocking way,
// returning the first send error it encountered.
//
// Like send, sendAll is idempotent, protecting against double delivery
// In practice, that means that in send, an event with an earlier rev compared to the watchers current rev is skipped.
// So in the same vain, we only increment the watchers current rev, if the events rev is actually older.
func (w *watcher) sendAll(events []Event) error {
	for _, ev := range events {
		if err := w.send(ev); err != nil {
			return err
		}
		if ev.Entry.ModRev > w.currentRev {
			w.currentRev = ev.Entry.ModRev
		}
	}
	return nil
}

func (w *watcher) matches(e Event) bool {
	if w.filter != nil {
		switch *w.filter {
		case FilterNoPut:
			if e.Kind == EventPut {
				return false
			}
		case FilterNoDelete:
			if e.Kind == EventDelete {
				return false
			}
		}
	}

	if len(w.keyEnd) == 0 {
		return bytes.Equal(e.Entry.Key, w.keyStart)
	}
	return bytes.Compare(e.Entry.Key, w.keyStart) >= 0 && bytes.Compare(e.Entry.Key, w.keyEnd) < 0
}
