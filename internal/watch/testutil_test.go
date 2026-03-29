package watch

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/types"
)

type mockStore struct {
	currentRev   int64
	compactedRev int64
	readerFn     func() mvcc.Reader
}

func (s *mockStore) RaftMeta() (uint64, uint64) { return 0, 0 }

func (s *mockStore) Ping() error { return nil }

func (s *mockStore) Revisions() (kv.Revision, int64) {
	return kv.Revision{Main: s.currentRev}, s.compactedRev
}

func (m *mockStore) NewReader() mvcc.Reader {
	if m.readerFn != nil {
		return m.readerFn()
	}
	return &mockReader{} // empty reader by default
}

func (m *mockStore) setCurrentRev(rev int64) {
	m.currentRev = rev
}

// Range and Get are left as noops since the watch system only uses RevisionRange.
type mockReader struct {
	entries []types.KvEntry
	err     error
}

func (r *mockReader) Range(key, end []byte, rev int64, limit int64) ([]types.KvEntry, int, int64, error) {
	return nil, 0, 0, nil
}

func (r *mockReader) Get(key []byte, rev int64) *types.KvEntry {
	return nil
}

func (r *mockReader) RevisionRange(startRev, endRev int64, limit int64) (out []types.KvEntry, err error) {
	if r.err != nil {
		return nil, r.err
	}
	// Filter entries to the requested range [startRev, endRev] just like the
	// real implementation would after the endRev++ adjustment.
	for _, e := range r.entries {
		if e.ModRev >= startRev && e.ModRev <= endRev {
			out = append(out, e)
		}
	}
	return
}

func putEntry(key, value string, modrev int64) types.KvEntry {
	return types.KvEntry{
		Key:       []byte(key),
		Value:     []byte(value),
		CreateRev: modrev,
		ModRev:    modrev,
		Version:   1,
	}
}

func tombstoneEntry(key string, modrev int64) types.KvEntry {
	return types.KvEntry{
		Key:    []byte(key),
		ModRev: modrev,
	}
}

func putEvent(key, value string, modrev int64) Event {
	return Event{
		Kind:  EventPut,
		Entry: putEntry(key, value, modrev),
	}
}

func deleteEvent(key string, modrev int64) Event {
	return Event{
		Kind:  EventDelete,
		Entry: tombstoneEntry(key, modrev),
	}
}

func drainEvents(c <-chan Event) (out []Event) {
	for {
		select {
		case ev, ok := <-c:
			if !ok {
				return
			}
			out = append(out, ev)
		default:
			return
		}
	}
}

func expectEvents(t *testing.T, c <-chan Event, n int) (out []Event) {
	t.Helper()
	out = make([]Event, 0, n)
	for range n {
		select {
		case ev, ok := <-c:
			if !ok {
				t.Fatalf("channel closed after %d events, expected %d", len(out), n)
			}
			out = append(out, ev)
		case <-time.After(time.Second * 2):
			t.Fatalf("timed out after %d events, expected %d", len(out), n)
		}
	}
	return out
}

func expectNoEvents(t *testing.T, c <-chan Event) {
	t.Helper()
	select {
	case ev, ok := <-c:
		if ok {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(50 * time.Millisecond):
		// ok
	}
}

func newTestHub(store *mockStore) *WatchHub {
	return &WatchHub{
		synced:   make(map[int64]*watcher),
		unsynced: make(map[int64]*watcher),
		store:    store,
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

func newTestUnsyncedLoop(hub *WatchHub) *UnsyncedWatcherLoop {
	ctx, cancel := context.WithCancel(context.Background())
	return &UnsyncedWatcherLoop{
		wh:               hub,
		unsyncedFailures: make(map[int64]int),
		ctx:              ctx,
		cancel:           cancel,
		logger:           *slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}
