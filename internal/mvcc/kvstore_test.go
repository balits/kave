package mvcc

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newTestKVStore(t *testing.T) *KVStore {
	reg := metrics.InitTestPrometheus()
	// b := backend.NewBackend(reg, storage.StorageOptions{
	// 	Kind:           storage.StorageKindInmemory,
	// 	InitialBuckets: schema.AllBuckets,
	// })
	b := backend.NewBackend(reg, storage.StorageOptions{
		Kind:           storage.StorageKindBoltdb,
		Dir:            t.TempDir(),
		InitialBuckets: schema.AllBuckets,
	})
	return NewKVStore(reg, slog.Default(), b)
}

func Test_KVStoreRevisionInitial(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

	currRev, _ := s.Revisions()
	if currRev.Main != 0 || currRev.Sub != 0 {
		t.Errorf("initial revision = %v, want {0,0}", currRev)
	}
}

func Test_KVStoreRevisionAfterWrites(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("b"), []byte("2"))
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 2 {
		t.Errorf("revision = %d, want 2", currRev.Main)
	}
}

func Test_KVStoreUpdateRaftMeta(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

	s.UpdateRaftMeta(100, 5)
	if s.applyIndex != 100 {
		t.Errorf("raftIndex = %d, want 100", s.applyIndex)
	}
	if s.raftTerm != 5 {
		t.Errorf("raftTerm = %d, want 5", s.raftTerm)
	}
}

func Test_KVStoreSnapshot(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

	snap := s.Snapshot()
	if snap.store != s {
		t.Error("snapshot should reference the store")
	}
}

func Test_KVStoreFullLifecycle(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

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

	currRev, _ := s.Revisions()
	if currRev.Main != 3 {
		t.Errorf("revision = %d, want 3", currRev.Main)
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
	s := newTestKVStore(t)
	defer s.Close()

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
	s := newTestKVStore(t)
	defer s.Close()

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
	s := newTestKVStore(t)
	defer s.Close()

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

func Test_KVStoreRestoreInmem(t *testing.T) {
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()
	s := NewKVStore(reg1, slog.Default(), backend.NewBackend(reg2, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	}))
	defer s.Close()
	s2 := NewKVStore(reg2, slog.Default(), backend.NewBackend(reg1, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	}))
	defer s2.Close()

	s.UpdateRaftMeta(10, 3)
	w := s.NewWriter()
	w.Put([]byte("rk"), []byte("rv"))
	w.End()

	var buf mockBuffer
	require.NoError(t, s.backend.Snapshot(&buf), "Snapshot error")

	require.NoError(t, s2.Restore(&buf), "Restore error")

	currRev, _ := s2.Revisions()
	require.Equal(t, int64(1), currRev.Main, "restored revision = %d, want 1", currRev.Main)

	r := s2.NewReader()
	entries, _, _, err := r.Range([]byte("rk"), nil, 0, 0)
	require.NoError(t, err, "Range error after restore")

	if len(entries) != 1 || string(entries[0].Value) != "rv" {
		t.Errorf("restored value = %v", entries)
	}

	require.Equal(t, uint64(10), s2.applyIndex, "restored raftIndex = %d, want 10", s2.applyIndex)
	require.Equal(t, uint64(3), s2.raftTerm, "restored raftTerm = %d, want 3", s2.raftTerm)
}

func Test_KVStoreRestoreBoltdb(t *testing.T) {
	// Each store needs its own directory — BoltDB holds a file lock
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	opts1 := storage.StorageOptions{Kind: storage.StorageKindBoltdb, Dir: dir1, InitialBuckets: schema.AllBuckets}
	opts2 := storage.StorageOptions{Kind: storage.StorageKindBoltdb, Dir: dir2, InitialBuckets: schema.AllBuckets}

	b1 := backend.NewBackend(prometheus.NewRegistry(), opts1)
	s1 := NewKVStore(prometheus.NewRegistry(), slog.Default(), b1)
	defer s1.Close()

	s1.UpdateRaftMeta(10, 3)
	w := s1.NewWriter()
	w.Put([]byte("k"), []byte("v"))
	w.End()

	// Snapshot into a buffer — don't touch files
	var buf bytes.Buffer
	snap := s1.Snapshot()
	sink := &sink{buf: &buf} // implements raft.SnapshotSink
	require.NoError(t, snap.Persist(sink))

	// Restore into a fresh store
	b2 := backend.NewBackend(prometheus.NewRegistry(), opts2)
	s2 := NewKVStore(prometheus.NewRegistry(), slog.Default(), b2)
	defer s2.Close()

	require.NoError(t, s2.Restore(&buf))

	// Assert state was fully transferred
	currRev, _ := s2.Revisions()
	require.Equal(t, int64(1), currRev.Main)

	r := s2.NewReader()
	entries, _, _, err := r.Range([]byte("k"), nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "v", string(entries[0].Value))

	idx, term := s2.RaftMeta()
	require.Equal(t, uint64(10), idx)
	require.Equal(t, uint64(3), term)
}

func Test_KVStoreRangeRejectsCompactedRev(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

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

func Test_KVStoreCompactDeletesEntries(t *testing.T) {
	s := newTestKVStore(t)
	defer s.Close()

	// Write key at revisions 1, 2, 3
	for i := range 3 {
		w := s.NewWriter()
		w.Put([]byte("k"), fmt.Appendf(nil, "v%d", i))
		w.End()
	}
	r := s.NewReader()
	entries, _, _, err := r.Range([]byte("k"), nil, 0, 0)
	t.Log(err)
	t.Log(entries)

	ch, err := s.Compact(2)
	require.NoError(t, err)
	<-ch

	rtx := s.backend.ReadTx()
	rtx.RLock()
	defer rtx.RUnlock()

	start := kv.EncodeRevision(kv.Revision{Main: 0}, kv.NewRevBytes())
	end := kv.EncodeRevision(kv.Revision{Main: 3}, kv.NewRevBytes())

	var found []kv.Revision
	rtx.UnsafeScan(schema.BucketKV, start, end, func(k, v []byte) error {
		bk := kv.DecodeKVBucketKey(k)
		found = append(found, bk.Revision)
		return nil
	})

	// Only {Main:2} should remain in the compacted range — {Main:1} must be deleted
	require.Len(t, found, 1, "expected exactly 1 entry retained in compacted range, got: %v", found)
	require.Equal(t, int64(2), found[0].Main)
}

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

// minimal raft.SnapshotSink for tests
type sink struct {
	buf *bytes.Buffer
}

func (s *sink) Write(p []byte) (int, error) {
	return s.buf.Write(p)
}

func (s *sink) Close() error {
	return nil
}

func (s *sink) ID() string {
	return "test"
}

func (s *sink) Cancel() error {
	return nil
}
