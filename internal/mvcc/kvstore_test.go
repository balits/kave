package mvcc

import (
	"log/slog"
	"os"
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
)

func newTestKVStore() *KVStore {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b := backend.NewBackend(storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: kv.AllBuckets,
	})
	return NewKVStore(logger, b)
}

// ==================== KVStore.Revision ====================

func Test_KVStoreRevisionInitial(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	rev := s.Revision()
	if rev.Main != 0 || rev.Sub != 0 {
		t.Errorf("initial revision = %v, want {0,0}", rev)
	}
}

func Test_KVStoreRevisionAfterWrites(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("b"), []byte("2"))
	w.End()

	if s.Revision().Main != 2 {
		t.Errorf("revision = %d, want 2", s.Revision().Main)
	}
}

// ==================== KVStore.UpdateRaftMeta ====================

func Test_KVStoreUpdateRaftMeta(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	s.UpdateRaftMeta(100, 5)
	if s.raftIndex != 100 {
		t.Errorf("raftIndex = %d, want 100", s.raftIndex)
	}
	if s.raftTerm != 5 {
		t.Errorf("raftTerm = %d, want 5", s.raftTerm)
	}
}

// ==================== KVStore.Snapshot ====================

func Test_KVStoreSnapshot(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	snap := s.Snapshot()
	if snap.store != s {
		t.Error("snapshot should reference the store")
	}
}

// ==================== KVStore end-to-end lifecycle ====================

func Test_KVStoreFullLifecycle(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("key1"), []byte("val1"))
	w.Put([]byte("key2"), []byte("val2"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("key1"), []byte("val1_updated"))
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("key2"))
	w.End()

	if s.Revision().Main != 3 {
		t.Errorf("revision = %d, want 3", s.Revision().Main)
	}

	r := s.NewReader()

	entries, _, _, _ := r.Range([]byte("key1"), nil, 0, 0)
	if len(entries) != 1 || string(entries[0].Value) != "val1_updated" {
		t.Errorf("key1 current = %v", entries)
	}

	entries, _, _, _ = r.Range([]byte("key1"), nil, 1, 0)
	if len(entries) != 1 || string(entries[0].Value) != "val1" {
		t.Errorf("key1 at rev 1 = %v", entries)
	}

	entries, _, _, _ = r.Range([]byte("key2"), nil, 0, 0)
	if len(entries) != 0 {
		t.Error("key2 should be deleted at current rev")
	}

	entries, _, _, _ = r.Range([]byte("key2"), nil, 1, 0)
	if len(entries) != 1 || string(entries[0].Value) != "val2" {
		t.Errorf("key2 at rev 1 = %v", entries)
	}
}

func Test_KVStoreVersionTracking(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	for range 5 {
		w := s.NewWriter()
		w.Put([]byte("k"), []byte("v"))
		w.End()
	}

	r := s.NewReader()
	entries, _, _, _ := r.Range([]byte("k"), nil, 0, 0)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Version != 5 {
		t.Errorf("version = %d, want 5", entries[0].Version)
	}
	if entries[0].CreateRev != 1 {
		t.Errorf("createRev = %d, want 1", entries[0].CreateRev)
	}
	if entries[0].ModRev != 5 {
		t.Errorf("modRev = %d, want 5", entries[0].ModRev)
	}
}

func Test_KVStoreSubRevisions(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.Put([]byte("b"), []byte("2"))
	w.Put([]byte("c"), []byte("3"))
	w.End()

	_, created, _, err := s.kvIndex.Get([]byte("a"), 1)
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	_ = created

	rev, _, _, err := s.kvIndex.Get([]byte("b"), 1)
	if err != nil {
		t.Fatalf("Get b: %v", err)
	}
	if rev.Sub != 1 {
		t.Errorf("b sub = %d, want 1", rev.Sub)
	}

	rev, _, _, err = s.kvIndex.Get([]byte("c"), 1)
	if err != nil {
		t.Fatalf("Get c: %v", err)
	}
	if rev.Sub != 2 {
		t.Errorf("c sub = %d, want 2", rev.Sub)
	}
}

func Test_KVStoreMultipleWritersSameKey(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v3"))
	w.End()

	r := s.NewReader()
	for rev := int64(1); rev <= 3; rev++ {
		entries, _, _, err := r.Range([]byte("k"), nil, rev, 0)
		if err != nil {
			t.Fatalf("Range at rev %d: %v", rev, err)
		}
		if len(entries) != 1 {
			t.Fatalf("rev %d: entries = %d, want 1", rev, len(entries))
		}
		expected := []byte("v" + string(rune('0'+rev)))
		if string(entries[0].Value) != string(expected) {
			t.Errorf("rev %d: value = %q, want %q", rev, entries[0].Value, expected)
		}
	}
}

// ==================== KVStore.Restore ====================

func Test_KVStoreRestore(t *testing.T) {
	s := newTestKVStore()

	s.UpdateRaftMeta(10, 3)
	w := s.NewWriter()
	w.Put([]byte("rk"), []byte("rv"))
	w.End()

	var buf mockBuffer
	if err := s.backend.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	s2 := newTestKVStore()
	defer s2.backend.Close()

	if err := s2.Restore(&buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if s2.Revision().Main != 1 {
		t.Errorf("restored revision = %d, want 1", s2.Revision().Main)
	}

	r := s2.NewReader()
	entries, _, _, err := r.Range([]byte("rk"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range after restore: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Value) != "rv" {
		t.Errorf("restored value = %v", entries)
	}

	if s2.raftIndex != 10 {
		t.Errorf("restored raftIndex = %d, want 10", s2.raftIndex)
	}
	if s2.raftTerm != 3 {
		t.Errorf("restored raftTerm = %d, want 3", s2.raftTerm)
	}
}

// ==================== KVStore compacted revision ====================

func Test_KVStoreRangeRejectsCompactedRev(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"))
	w.End()

	s.revMu.Lock()
	s.compactedMainRev = 5
	s.revMu.Unlock()

	r := s.NewReader()
	_, _, _, err := r.Range([]byte("k"), nil, 1, 0)
	if err == nil {
		t.Error("expected compacted error for rev < compactedMainRev")
	}
}

// ==================== mockBuffer for snapshot/restore ====================

type mockBuffer struct {
	data []byte
	pos  int
}

func (m *mockBuffer) Write(p []byte) (n int, err error) {
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *mockBuffer) Read(p []byte) (n int, err error) {
	if m.pos >= len(m.data) {
		return 0, kv.ErrCompacted // any io.EOF-like
	}
	n = copy(p, m.data[m.pos:])
	m.pos += n
	if m.pos >= len(m.data) {
		return n, nil
	}
	return n, nil
}
