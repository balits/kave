package bytestore

import (
	"fmt"
	"testing"

	"github.com/balits/kave/internal/storage"
)

const bucket storage.Bucket = "test"

func TestRecordPut(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordPut(bucket, []byte("k"), []byte("v"))

	puts := wc.Puts()
	if len(puts) != 1 {
		t.Fatalf("puts has %d buckets, want 1", len(puts))
	}
	if v, ok := puts[bucket]["k"]; !ok || string(v) != "v" {
		t.Errorf("puts[test][k] = %q, want %q", v, "v")
	}
}

func TestRecordPutOverwrite(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordPut(bucket, []byte("k"), []byte("v1"))
	wc.RecordPut(bucket, []byte("k"), []byte("v2"))

	v := wc.Puts()[bucket]["k"]
	if string(v) != "v2" {
		t.Errorf("overwrite: got %q, want %q", v, "v2")
	}
}

func TestRecordDelete(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordDelete(bucket, []byte("k"))

	dels := wc.Deletes()
	if len(dels) != 1 {
		t.Fatalf("deletes has %d buckets, want 1", len(dels))
	}
	if _, ok := dels[bucket]["k"]; !ok {
		t.Error("delete not recorded")
	}
}

func TestPutUndoesDelete(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordDelete(bucket, []byte("k"))
	wc.RecordPut(bucket, []byte("k"), []byte("v"))

	if _, ok := wc.Deletes()[bucket]["k"]; ok {
		t.Error("put should remove key from deletes")
	}
	if v, ok := wc.Puts()[bucket]["k"]; !ok || string(v) != "v" {
		t.Errorf("put should be recorded, got %q", v)
	}
}

func TestDeleteUndoesPut(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordPut(bucket, []byte("k"), []byte("v"))
	wc.RecordDelete(bucket, []byte("k"))

	if _, ok := wc.Puts()[bucket]["k"]; ok {
		t.Error("delete should remove key from puts")
	}
	if _, ok := wc.Deletes()[bucket]["k"]; !ok {
		t.Error("delete should be recorded")
	}
}

func TestInterleavedPutDelete(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordPut(bucket, []byte("k"), []byte("v1"))
	wc.RecordDelete(bucket, []byte("k"))
	wc.RecordPut(bucket, []byte("k"), []byte("v2"))

	if _, ok := wc.Deletes()[bucket]["k"]; ok {
		t.Error("final put should remove key from deletes")
	}
	v := wc.Puts()[bucket]["k"]
	if string(v) != "v2" {
		t.Errorf("final value = %q, want %q", v, "v2")
	}
}

func TestDeletePutDelete(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordDelete(bucket, []byte("k"))
	wc.RecordPut(bucket, []byte("k"), []byte("v"))
	wc.RecordDelete(bucket, []byte("k"))

	if _, ok := wc.Puts()[bucket]["k"]; ok {
		t.Error("final delete should remove key from puts")
	}
	if _, ok := wc.Deletes()[bucket]["k"]; !ok {
		t.Error("final delete should be recorded")
	}
}

func TestMultipleBuckets(t *testing.T) {
	wc := NewWriteCollector()
	b1 := storage.Bucket("b1")
	b2 := storage.Bucket("b2")

	wc.RecordPut(b1, []byte("k"), []byte("v1"))
	wc.RecordPut(b2, []byte("k"), []byte("v2"))

	if string(wc.Puts()[b1]["k"]) != "v1" {
		t.Error("b1 value wrong")
	}
	if string(wc.Puts()[b2]["k"]) != "v2" {
		t.Error("b2 value wrong")
	}
}

func TestDeleteInOneBucketDoesNotAffectOther(t *testing.T) {
	wc := NewWriteCollector()
	b1 := storage.Bucket("b1")
	b2 := storage.Bucket("b2")

	wc.RecordPut(b1, []byte("k"), []byte("v1"))
	wc.RecordPut(b2, []byte("k"), []byte("v2"))
	wc.RecordDelete(b1, []byte("k"))

	if _, ok := wc.Puts()[b1]["k"]; ok {
		t.Error("b1.k should be deleted from puts")
	}
	if _, ok := wc.Puts()[b2]["k"]; !ok {
		t.Error("b2.k should still be in puts")
	}
}

func TestReset(t *testing.T) {
	wc := NewWriteCollector()
	wc.RecordPut(bucket, []byte("k1"), []byte("v1"))
	wc.RecordDelete(bucket, []byte("k2"))

	wc.Reset()

	if len(wc.Puts()) != 0 {
		t.Errorf("puts not empty after reset: %v", wc.Puts())
	}
	if len(wc.Deletes()) != 0 {
		t.Errorf("deletes not empty after reset: %v", wc.Deletes())
	}
}

func TestManyKeys(t *testing.T) {
	wc := NewWriteCollector()
	for i := 0; i < 100; i++ {
		wc.RecordPut(bucket, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	if len(wc.Puts()[bucket]) != 100 {
		t.Errorf("puts has %d keys, want 100", len(wc.Puts()[bucket]))
	}

	for i := 0; i < 50; i++ {
		wc.RecordDelete(bucket, []byte(fmt.Sprintf("k%d", i)))
	}
	if len(wc.Puts()[bucket]) != 50 {
		t.Errorf("after deleting 50: puts has %d keys, want 50", len(wc.Puts()[bucket]))
	}
	if len(wc.Deletes()[bucket]) != 50 {
		t.Errorf("deletes has %d keys, want 50", len(wc.Deletes()[bucket]))
	}
}
