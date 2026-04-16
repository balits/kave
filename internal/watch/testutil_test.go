package watch

import (
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
)

type mockStore struct {
	currentRev   int64
	compactedRev int64
}

func (s *mockStore) Meta() (kv.Revision, int64, uint64, uint64) {
	return kv.Revision{Main: s.currentRev}, s.compactedRev, 0, 0
}

func (s *mockStore) RaftMeta() (uint64, uint64) { return 0, 0 }
func (s *mockStore) Ping() error                { return nil }

func (s *mockStore) NewReader() mvcc.Reader {
	return &mockReader{s: s}
}

// mockReader satisfies mvcc.Reader
type mockReader struct {
	s *mockStore
}

func (r *mockReader) Revisions() (kv.Revision, int64) {
	return kv.Revision{Main: r.s.currentRev}, r.s.compactedRev
}

func (r *mockReader) Range(key, end []byte, rev int64, limit int64) ([]*kv.Entry, int, int64, error) {
	if rev != 0 && rev < r.s.compactedRev {
		return nil, 0, 0, kv.ErrCompacted
	}

	// For the purpose of WatchHub.NewWatcher logic:
	// If it's a single-key read (end == nil), NewWatcher expects 'rev' to be
	// the latest revision of that key. To simulate a "caught up" store,
	// we just return the store's current main revision.
	return nil, 0, r.s.currentRev, nil
}

func (r *mockReader) Get(key []byte, rev int64) *kv.Entry {
	return nil
}

func putEntry(key, value string, modrev int64) *kv.Entry {
	return &kv.Entry{
		Key:       []byte(key),
		Value:     []byte(value),
		CreateRev: modrev,
		ModRev:    modrev,
		Version:   1,
	}
}

func testTombstoneEntry(key string, modrev int64) *kv.Entry {
	return &kv.Entry{
		Key:    []byte(key),
		ModRev: modrev,
	}
}

func testPutEvent(key, value string, modrev int64) kv.Event {
	return kv.Event{
		Kind:  kv.EventPut,
		Entry: putEntry(key, value, modrev),
	}
}

func testDeleteEvent(key string, modrev int64) kv.Event {
	return kv.Event{
		Kind:  kv.EventDelete,
		Entry: testTombstoneEntry(key, modrev),
	}
}

func drainEvents(c <-chan kv.Event) (out []kv.Event) {
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

func expectEvents(t *testing.T, c <-chan kv.Event, n int) (out []kv.Event) {
	t.Helper()
	out = make([]kv.Event, 0, n)
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

func expectNoEvents(t *testing.T, c <-chan kv.Event) {
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

func newMockHub(store *mockStore) *WatchHub {

	return &WatchHub{
		synced:   make(map[int64]*watcher),
		unsynced: make(map[int64]*watcher),
		store:    store,
		logger:   slog.Default(),
	}
}
