package watch

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/balits/kave/internal/kv"
)

const (
	watcherChannelBufferSize = 128
)

var (
	// ErrWatcherOverloaded is returned when a watcher's channel is full and it cannot receive new events.
	ErrWatcherOverloaded = errors.New("watcher is overloaded")
	// ErrWatcherIDConflict is returned if the requested watcher id is already in use.
	ErrWatcherIDConflict = fmt.Errorf("watch ID conflict")
)

type watcher struct {
	id         int64
	keyStart   []byte
	keyEnd     []byte
	startRev   int64
	currentRev int64
	filter     *kv.EventFilter // either NO_PUT or NO_DELETE, or empty for no filter
	prevEntry  bool
	ctx        context.Context
	cancel     context.CancelCauseFunc
	c          chan kv.Event
}

func (w *watcher) String() string {
	return fmt.Sprintf("watcher(ID=%d, Start=%d, Key=%s, End=%s)",
		w.id,
		w.startRev,
		string(w.keyStart),
		string(w.keyEnd),
	)
}

func (w *watcher) close(cause error) {
	w.cancel(cause)
	close(w.c)
}

// send is idempotent, because of the race between OnCommit and UnsynchedWatcherLoop, protecting against double delivery.
// In practice, an event with an earlier rev compared to the watchers current rev is skipped.
func (w *watcher) send(e kv.Event) (sent bool, err error) {
	if w.ctx.Err() != nil {
		return false, w.ctx.Err()
	}

	if !w.matches(e) {
		return false, nil
	}

	if e.Entry.ModRev <= w.currentRev {
		return false, nil
	}

	select {
	case <-w.ctx.Done():
		return false, w.ctx.Err()
	case w.c <- e:
		return true, nil
	default:
		return false, ErrWatcherOverloaded
	}
}

// sendAll sends every event it gets to the watcher in a non blocking way,
// returning the first send error it encountered.
//
// Like send, sendAll is idempotent, protecting against double delivery
// In practice, that means that in send, an event with an earlier rev compared to the watchers current rev is skipped.
// So in the same vain, we only increment the watchers current rev if the events rev is actually older.
func (w *watcher) sendAll(events []kv.Event) error {
	for _, ev := range events {
		sent, err := w.send(ev)
		if err != nil {
			return err
		}
		if !sent {
			continue
		}

		if ev.Entry.ModRev > w.currentRev {
			w.currentRev = ev.Entry.ModRev
		}
	}
	return nil
}

func (w *watcher) matches(e kv.Event) bool {
	if w.filter != nil {
		switch *w.filter {
		case kv.FilterNoPut:
			if e.Kind == kv.EventPut {
				return false
			}
		case kv.FilterNoDelete:
			if e.Kind == kv.EventDelete {
				return false
			}
		}
	}

	if len(w.keyStart) == 0 && len(w.keyEnd) == 0 {
		return true // Watch Key [nil..nil) matches everything
	}

	if len(w.keyEnd) == 0 {
		return bytes.Equal(e.Entry.Key, w.keyStart)
	}
	return bytes.Compare(e.Entry.Key, w.keyStart) >= 0 && bytes.Compare(e.Entry.Key, w.keyEnd) < 0
}
