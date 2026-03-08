package mvcc

import (
	"bytes"
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/stretchr/testify/require"
)

// ==================== Writer.Put ====================

func Test_WriterPutSingleKey(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	rev, err := w.Put([]byte("foo"), []byte("bar"))
	require.NoError(t, err, "unexpected error from Put()")
	w.End()

	if rev.Main != 1 {
		t.Errorf("rev.Main = %d, want 1", rev.Main)
	}

	currRev, _ := s.Revisions()
	if currRev.Main != 1 {
		t.Errorf("store revision = %d, want 1", currRev.Main)
	}
}

func Test_WriterPutMultipleKeys(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.Put([]byte("b"), []byte("2"))
	w.Put([]byte("c"), []byte("3"))
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 1 {
		t.Errorf("revision = %d, want 1 (single writer = single main rev)", currRev.Main)
	}

	changes := w.Changes()
	if len(changes) != 3 {
		t.Fatalf("changes = %d, want 3", len(changes))
	}
}

func Test_WriterPutSameKeyTwice(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"))
	w.End()

	rev, _ := s.Revisions()
	if rev.Main != 2 {
		t.Errorf("revision = %d, want 2", rev.Main)
	}
}

func Test_WriterPutPreservesCreateRev(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"))
	w.End()

	changes := w.Changes()
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if changes[0].CreateRev != 1 {
		t.Errorf("CreateRev = %d, want 1 (should be preserved from first put)", changes[0].CreateRev)
	}
	if changes[0].Version != 2 {
		t.Errorf("Version = %d, want 2", changes[0].Version)
	}
}

func Test_WriterPutEntryFields(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("mykey"), []byte("myval"))
	w.End()

	changes := w.Changes()
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	e := changes[0]
	if !bytes.Equal(e.Key, []byte("mykey")) {
		t.Errorf("Key = %q, want %q", e.Key, "mykey")
	}
	if !bytes.Equal(e.Value, []byte("myval")) {
		t.Errorf("Value = %q, want %q", e.Value, "myval")
	}
	if e.CreateRev != 1 {
		t.Errorf("CreateRev = %d, want 1", e.CreateRev)
	}
	if e.ModRev != 1 {
		t.Errorf("ModRev = %d, want 1", e.ModRev)
	}
	if e.Version != 1 {
		t.Errorf("Version = %d, want 1", e.Version)
	}
}

// ==================== Writer.Revision ====================

func Test_WriterRevisionReturnsStartRev(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.End()

	w = s.NewWriter()
	startRev := w.Revision()
	if startRev.Main != 1 {
		t.Errorf("writer start rev = %d, want 1", startRev.Main)
	}
	w.End()
}

// ==================== Writer.DeleteKey ====================

func Test_WriterDeleteKey(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"))
	w.End()

	w = s.NewWriter()
	count, rev, err := w.DeleteKey([]byte("foo"))
	require.NoError(t, err, "unexpected error from DeleteKey()")
	w.End()

	if count != 1 {
		t.Errorf("deleted = %d, want 1", count)
	}
	if rev.Main != 2 {
		t.Errorf("rev = %d, want 2", rev.Main)
	}
}

func Test_WriterDeleteKeyNonExistent(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	count, rev, err := w.DeleteKey([]byte("nope"))
	require.NoError(t, err, "unexpected error from DeleteKey()")
	w.End()

	if count != 0 {
		t.Errorf("deleted = %d, want 0", count)
	}
	if rev.Main != 0 {
		t.Errorf("rev.Main = %d, want 0 (no changes)", rev.Main)
	}
}

func Test_WriterDeleteKeyThenReCreate(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("v1"))
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	w = s.NewWriter()
	w.Put([]byte("foo"), []byte("v2"))
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 3 {
		t.Errorf("revision = %d, want 3", currRev.Main)
	}

	changes := w.Changes()
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if changes[0].CreateRev != 3 {
		t.Errorf("re-created key CreateRev = %d, want 3", changes[0].CreateRev)
	}
	if changes[0].Version != 1 {
		t.Errorf("re-created key Version = %d, want 1", changes[0].Version)
	}
}

// ==================== Writer.DeleteRange ====================

func Test_WriterDeleteRange(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.Put([]byte("b"), []byte("2"))
	w.Put([]byte("c"), []byte("3"))
	w.Put([]byte("d"), []byte("4"))
	w.End()

	w = s.NewWriter()
	count, rev, err := w.DeleteRange([]byte("b"), []byte("d"))
	require.NoError(t, err, "unexpected error from DeleteRange()")
	w.End()

	if count != 2 {
		t.Errorf("deleted = %d, want 2 (b and c)", count)
	}
	if rev.Main != 2 {
		t.Errorf("rev = %d, want 2", rev.Main)
	}
}

func Test_WriterDeleteRangeEmpty(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.End()

	w = s.NewWriter()
	count, _, err := w.DeleteRange([]byte("x"), []byte("z"))
	require.NoError(t, err, "unexpected error from DeleteRange()")
	w.End()

	if count != 0 {
		t.Errorf("deleted = %d, want 0", count)
	}
}

// ==================== Writer.Changes ====================

func Test_WriterChangesEmpty(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.End()

	if len(w.Changes()) != 0 {
		t.Errorf("changes = %d, want 0", len(w.Changes()))
	}
}

func Test_WriterChangesIncludesTombstones(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"))
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	changes := w.Changes()
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if !bytes.Equal(changes[0].Key, []byte("foo")) {
		t.Errorf("tombstone key = %q, want %q", changes[0].Key, "foo")
	}
}

// ==================== Writer.End revision bump ====================

func Test_WriterEndNoChangesNoRevBump(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 0 {
		t.Errorf("revision = %d, want 0 (no changes = no bump)", currRev.Main)
	}
}

func Test_WriterEndBumpsRevisionOnce(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"))
	w.Put([]byte("b"), []byte("2"))
	w.Put([]byte("c"), []byte("3"))
	w.End()

	currRev, _ := s.Revisions()
	if currRev.Main != 1 {
		t.Errorf("revision = %d, want 1 (one writer = one bump)", currRev.Main)
	}
}

func Test_WriterEndPersistsRaftMeta(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	s.UpdateRaftMeta(42, 7)

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"))
	w.End()

	rtx := s.backend.ReadTx()
	rtx.RLock()
	indexBytes, _ := rtx.UnsafeGet(kv.BucketMeta, kv.MetaKeyRaftApplyIndex)
	termBytes, _ := rtx.UnsafeGet(kv.BucketMeta, kv.MetaKeyRaftTerm)
	rtx.RUnlock()

	idx, _ := kv.DecodeUint64(indexBytes)
	term, _ := kv.DecodeUint64(termBytes)

	if idx != 42 {
		t.Errorf("raft index = %d, want 42", idx)
	}
	if term != 7 {
		t.Errorf("raft term = %d, want 7", term)
	}
}

func Test_WriterEndNoRaftMetaWhenZero(t *testing.T) {
	s := newTestKVStore()
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"))
	w.End()

	rtx := s.backend.ReadTx()
	rtx.RLock()
	indexBytes, _ := rtx.UnsafeGet(kv.BucketMeta, kv.MetaKeyRaftApplyIndex)
	rtx.RUnlock()

	if indexBytes != nil {
		t.Error("raft meta should not be persisted when raftIndex == 0")
	}
}
