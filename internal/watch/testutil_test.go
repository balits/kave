package watch

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
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

type mockReader struct {
	s       *mockStore
	entries []*kv.Entry
	err     error
}

func (r *mockReader) Revisions() (kv.Revision, int64) {
	return r.s.Revisions()
}

func (r *mockReader) Range(key, end []byte, rev int64, limit int64) (out []*kv.Entry, count int, highestRev int64, err error) {
	if rev < r.s.compactedRev {
		return nil, 0, 0, kv.ErrCompacted
	}

	match := func(e *kv.Entry) bool {
		if len(end) == 0 {
			return bytes.Equal(e.Key, key)
		}
		return bytes.Compare(e.Key, key) >= 0 && bytes.Compare(e.Key, end) < 0
	}
	for _, e := range r.entries {
		if match(e) {
			out = append(out, e)
			if highestRev < e.ModRev {
				highestRev = e.ModRev
			}
		}
	}

	return out, len(out), highestRev, nil
}

func (r *mockReader) Get(key []byte, rev int64) *kv.Entry {
	e, _, _, err := r.Range(key, nil, rev, 1)
	if err != nil {
		return nil
	}
	if len(e) == 1 {
		return e[0]
	}
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
