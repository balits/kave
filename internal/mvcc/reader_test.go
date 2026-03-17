package mvcc

import (
	"bytes"
	"testing"
)

func TestReaderRangeSingleKey(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	r := s.NewReader()
	entries, total, curRev, err := r.Range([]byte("foo"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1", total)
	}
	if curRev != 1 {
		t.Errorf("curRev = %d, want 1", curRev)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("bar")) {
		t.Errorf("value = %q, want %q", entries[0].Value, "bar")
	}
}

func TestReaderRangeAtSpecificRev(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.Put([]byte("foo"), []byte("v2"), 0)
	w.End()

	r := s.NewReader()
	entries, _, _, err := r.Range([]byte("foo"), nil, 1, 0)
	if err != nil {
		t.Fatalf("Range at rev 1: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("v1")) {
		t.Errorf("value at rev 1 = %q, want %q", entries[0].Value, "v1")
	}

	entries, _, _, err = r.Range([]byte("foo"), nil, 2, 0)
	if err != nil {
		t.Fatalf("Range at rev 2: %v", err)
	}
	if !bytes.Equal(entries[0].Value, []byte("v2")) {
		t.Errorf("value at rev 2 = %q, want %q", entries[0].Value, "v2")
	}
}

func TestReaderRangeRevZeroUsesCurrentRev(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v1"), 0)
	w.End()

	w = s.NewWriter()
	w.Put([]byte("k"), []byte("v2"), 0)
	w.End()

	r := s.NewReader()
	entries, _, _, err := r.Range([]byte("k"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if !bytes.Equal(entries[0].Value, []byte("v2")) {
		t.Errorf("rev=0 should return latest: got %q, want %q", entries[0].Value, "v2")
	}
}

func TestReaderRangeFutureRevError(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("k"), []byte("v"), 0)
	w.End()

	r := s.NewReader()
	_, _, _, err := r.Range([]byte("k"), nil, 999, 0)
	if err == nil {
		t.Error("expected error for future revision")
	}
}

func TestReaderRangeMultipleKeys(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.Put([]byte("d"), []byte("4"), 0)
	w.End()

	r := s.NewReader()
	entries, total, _, err := r.Range([]byte("b"), []byte("d"), 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
}

func TestReaderRangeWithLimit(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("a"), []byte("1"), 0)
	w.Put([]byte("b"), []byte("2"), 0)
	w.Put([]byte("c"), []byte("3"), 0)
	w.End()

	r := s.NewReader()
	entries, total, _, err := r.Range([]byte("a"), []byte("d"), 0, 2)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("entries = %d, want 2 (limited)", len(entries))
	}
	if total != 3 {
		t.Errorf("total = %d, want 3 (unlimited count)", total)
	}
}

func TestReaderRangeNonExistentKey(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	r := s.NewReader()
	entries, total, _, err := r.Range([]byte("zzz"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(entries) != 0 || total != 0 {
		t.Errorf("entries=%d total=%d, want 0 0", len(entries), total)
	}
}

func TestReaderRangeDeletedKey(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	r := s.NewReader()
	entries, _, _, err := r.Range([]byte("foo"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("deleted key should not appear: entries = %d", len(entries))
	}
}

func TestReaderRangeDeletedKeyAtOldRev(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	w := s.NewWriter()
	w.Put([]byte("foo"), []byte("bar"), 0)
	w.End()

	w = s.NewWriter()
	w.DeleteKey([]byte("foo"))
	w.End()

	r := s.NewReader()
	entries, _, _, err := r.Range([]byte("foo"), nil, 1, 0)
	if err != nil {
		t.Fatalf("Range at old rev: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("should see key at rev before delete: entries = %d", len(entries))
	}
}

func TestReaderRangeEmptyStore(t *testing.T) {
	s := newTestKVStore(t)
	defer s.backend.Close()

	r := s.NewReader()
	entries, total, _, err := r.Range([]byte("anything"), nil, 0, 0)
	if err != nil {
		t.Fatalf("Range on empty store: %v", err)
	}
	if len(entries) != 0 || total != 0 {
		t.Errorf("empty store: entries=%d total=%d", len(entries), total)
	}
}
