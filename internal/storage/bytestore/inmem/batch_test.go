package inmem

import (
	"bytes"
	"fmt"
	"testing"
)

// ==================== Batch Put + Commit ====================

func TestBatchPutAndCommit(t *testing.T) {
	s := newTestStore()
	b, err := s.NewBatch()
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}

	b.Put(testBucket, []byte("k1"), []byte("v1"))
	b.Put(testBucket, []byte("k2"), []byte("v2"))

	val, _ := s.Get(testBucket, []byte("k1"))
	if val != nil {
		t.Error("value visible before commit")
	}

	if err := b.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	v1, _ := s.Get(testBucket, []byte("k1"))
	v2, _ := s.Get(testBucket, []byte("k2"))
	if !bytes.Equal(v1, []byte("v1")) {
		t.Errorf("k1 = %q, want %q", v1, "v1")
	}
	if !bytes.Equal(v2, []byte("v2")) {
		t.Errorf("k2 = %q, want %q", v2, "v2")
	}
}

// ==================== Batch Delete + Commit ====================

func TestBatchDeleteAndCommit(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k"), []byte("v"))

	b, _ := s.NewBatch()
	b.Delete(testBucket, []byte("k"))

	val, _ := s.Get(testBucket, []byte("k"))
	if val == nil {
		t.Error("value should still exist before commit")
	}

	b.Commit()

	val, _ = s.Get(testBucket, []byte("k"))
	if val != nil {
		t.Errorf("key still present after batch delete+commit: %q", val)
	}
}

// ==================== Batch Abort ====================

func TestBatchAbort(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("existing"), []byte("yes"))

	b, _ := s.NewBatch()
	b.Put(testBucket, []byte("aborted"), []byte("no"))
	b.Delete(testBucket, []byte("existing"))
	b.Abort()

	val, _ := s.Get(testBucket, []byte("aborted"))
	if val != nil {
		t.Errorf("aborted put should not exist, got %q", val)
	}

	existing, _ := s.Get(testBucket, []byte("existing"))
	if !bytes.Equal(existing, []byte("yes")) {
		t.Errorf("aborted delete should not affect existing key, got %q", existing)
	}
}

// ==================== Batch closed errors ====================

func TestBatchPutAfterCommit(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Put(testBucket, []byte("k"), []byte("v"))
	b.Commit()

	err := b.Put(testBucket, []byte("k2"), []byte("v2"))
	if err == nil {
		t.Fatal("expected error on Put after Commit")
	}
}

func TestBatchDeleteAfterCommit(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Commit()

	err := b.Delete(testBucket, []byte("k"))
	if err == nil {
		t.Fatal("expected error on Delete after Commit")
	}
}

func TestBatchCommitAfterCommit(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Commit()

	err := b.Commit()
	if err == nil {
		t.Fatal("expected error on double Commit")
	}
}

func TestBatchPutAfterAbort(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Abort()

	err := b.Put(testBucket, []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected error on Put after Abort")
	}
}

func TestBatchDeleteAfterAbort(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Abort()

	err := b.Delete(testBucket, []byte("k"))
	if err == nil {
		t.Fatal("expected error on Delete after Abort")
	}
}

func TestBatchCommitAfterAbort(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Abort()

	err := b.Commit()
	if err == nil {
		t.Fatal("expected error on Commit after Abort")
	}
}

func TestBatchAbortAfterAbort(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Abort()
	b.Abort()
}

// ==================== Batch overwrites within same batch ====================

func TestBatchPutOverwritesSameBatch(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Put(testBucket, []byte("k"), []byte("v1"))
	b.Put(testBucket, []byte("k"), []byte("v2"))
	b.Commit()

	val, _ := s.Get(testBucket, []byte("k"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("overwrite in batch: got %q, want %q", val, "v2")
	}
}

func TestBatchDeleteUndoesPut(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Put(testBucket, []byte("k"), []byte("v"))
	b.Delete(testBucket, []byte("k"))
	b.Commit()

	val, _ := s.Get(testBucket, []byte("k"))
	if val != nil {
		t.Errorf("delete should undo put in same batch, got %q", val)
	}
}

func TestBatchPutUndoesDelete(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k"), []byte("original"))

	b, _ := s.NewBatch()
	b.Delete(testBucket, []byte("k"))
	b.Put(testBucket, []byte("k"), []byte("restored"))
	b.Commit()

	val, _ := s.Get(testBucket, []byte("k"))
	if !bytes.Equal(val, []byte("restored")) {
		t.Errorf("put should undo delete in same batch: got %q, want %q", val, "restored")
	}
}

// ==================== Batch multiple buckets ====================

func TestBatchMultipleBuckets(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	b.Put(testBucket, []byte("dk"), []byte("dv"))
	b.Put(metaBucket, []byte("mk"), []byte("mv"))
	b.Commit()

	dv, _ := s.Get(testBucket, []byte("dk"))
	mv, _ := s.Get(metaBucket, []byte("mk"))
	if !bytes.Equal(dv, []byte("dv")) {
		t.Errorf("data = %q, want %q", dv, "dv")
	}
	if !bytes.Equal(mv, []byte("mv")) {
		t.Errorf("meta = %q, want %q", mv, "mv")
	}
}

// ==================== Batch many keys ====================

func TestBatchManyKeys(t *testing.T) {
	s := newTestStore()
	b, _ := s.NewBatch()
	n := 500
	for i := 0; i < n; i++ {
		b.Put(testBucket, []byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}
	b.Commit()

	var count int
	s.Scan(testBucket, func(k, v []byte) bool {
		count++
		return true
	})
	if count != n {
		t.Errorf("scanned %d, want %d", count, n)
	}
}

// ==================== Batch atomicity ====================

func TestBatchAtomicityOnAbort(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("a"), []byte("1"))
	s.Put(testBucket, []byte("b"), []byte("2"))

	b, _ := s.NewBatch()
	b.Put(testBucket, []byte("a"), []byte("modified"))
	b.Delete(testBucket, []byte("b"))
	b.Put(testBucket, []byte("c"), []byte("new"))
	b.Abort()

	a, _ := s.Get(testBucket, []byte("a"))
	bv, _ := s.Get(testBucket, []byte("b"))
	c, _ := s.Get(testBucket, []byte("c"))

	if !bytes.Equal(a, []byte("1")) {
		t.Errorf("a = %q, want %q", a, "1")
	}
	if !bytes.Equal(bv, []byte("2")) {
		t.Errorf("b = %q, want %q", bv, "2")
	}
	if c != nil {
		t.Errorf("c should not exist, got %q", c)
	}
}

// ==================== Sequential batches ====================

func TestSequentialBatches(t *testing.T) {
	s := newTestStore()

	b1, _ := s.NewBatch()
	b1.Put(testBucket, []byte("k"), []byte("v1"))
	b1.Commit()

	b2, _ := s.NewBatch()
	b2.Put(testBucket, []byte("k"), []byte("v2"))
	b2.Commit()

	b3, _ := s.NewBatch()
	b3.Delete(testBucket, []byte("k"))
	b3.Commit()

	b4, _ := s.NewBatch()
	b4.Put(testBucket, []byte("k"), []byte("v3"))
	b4.Commit()

	val, _ := s.Get(testBucket, []byte("k"))
	if !bytes.Equal(val, []byte("v3")) {
		t.Errorf("after 4 batches: got %q, want %q", val, "v3")
	}
}