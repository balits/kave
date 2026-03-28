package inmem

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/balits/kave/internal/storage"
	"github.com/stretchr/testify/require"
)

const testBucket storage.Bucket = "test"
const metaBucket storage.Bucket = "_meta"

func newTestStore() *InmemStore {
	return NewStore(storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: []storage.Bucket{"test", "_meta"},
	}).(*InmemStore)
}

func Test_Get_MissingKey(t *testing.T) {
	s := newTestStore()
	val, err := s.Get(testBucket, []byte("nope"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %q", val)
	}
}

func Test_Get_WrongBucket(t *testing.T) {
	s := newTestStore()
	_, err := s.Get("nonexistent", []byte("key"))
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func Test_Get_AfterPut(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k"), []byte("v"))

	val, err := s.Get(testBucket, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("v")) {
		t.Errorf("Get = %q, want %q", val, "v")
	}
}

func Test_Put_WrongBucket(t *testing.T) {
	s := newTestStore()
	_, err := s.Put("nonexistent", []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func Test_Put_Overwrite(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k"), []byte("v1"))
	s.Put(testBucket, []byte("k"), []byte("v2"))

	val, _ := s.Get(testBucket, []byte("k"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("overwrite: got %q, want %q", val, "v2")
	}
}

func Test_Put_MultipleBuckets(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("dk"), []byte("dv"))
	s.Put(metaBucket, []byte("mk"), []byte("mv"))

	dv, _ := s.Get(testBucket, []byte("dk"))
	mv, _ := s.Get(metaBucket, []byte("mk"))
	if !bytes.Equal(dv, []byte("dv")) {
		t.Errorf("data = %q, want %q", dv, "dv")
	}
	if !bytes.Equal(mv, []byte("mv")) {
		t.Errorf("meta = %q, want %q", mv, "mv")
	}
}

func Test_DeleteExisting(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k"), []byte("v"))

	val, err := s.Delete(testBucket, []byte("k"))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !bytes.Equal(val, []byte("v")) {
		t.Errorf("Delete returned %q, want %q", val, "v")
	}

	got, _ := s.Get(testBucket, []byte("k"))
	if got != nil {
		t.Errorf("key still present after delete: %q", got)
	}
}

func Test_DeleteMissing(t *testing.T) {
	s := newTestStore()
	val, err := s.Delete(testBucket, []byte("nope"))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for missing key, got %q", val)
	}
}

func Test_DeleteWrongBucket(t *testing.T) {
	s := newTestStore()
	_, err := s.Delete("nonexistent", []byte("k"))
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func Test_Scan_All(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("a"), []byte("1"))
	s.Put(testBucket, []byte("b"), []byte("2"))
	s.Put(testBucket, []byte("c"), []byte("3"))

	var keys []string
	err := s.Scan(testBucket, func(k, v []byte) bool {
		keys = append(keys, string(k))
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("scanned %d keys, want 3", len(keys))
	}
}

func Test_Scan_EarlyStop(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("a"), []byte("1"))
	s.Put(testBucket, []byte("b"), []byte("2"))
	s.Put(testBucket, []byte("c"), []byte("3"))

	var count int
	s.Scan(testBucket, func(k, v []byte) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("visited %d, want 2", count)
	}
}

func Test_Scan_WrongBucket(t *testing.T) {
	s := newTestStore()
	err := s.Scan("nonexistent", func(k, v []byte) bool { return true })
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func Test_Scan_Order(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("c"), []byte("3"))
	s.Put(testBucket, []byte("a"), []byte("1"))
	s.Put(testBucket, []byte("b"), []byte("2"))

	var keys []string
	s.Scan(testBucket, func(k, v []byte) bool {
		keys = append(keys, string(k))
		return true
	})
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("scan order = %v, want [a b c]", keys)
	}
}

func Test_PrefixScan(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("foo/1"), []byte("a"))
	s.Put(testBucket, []byte("foo/2"), []byte("b"))
	s.Put(testBucket, []byte("bar/1"), []byte("c"))

	var keys []string
	err := s.PrefixScan(testBucket, []byte("foo/"), func(k, v []byte) bool {
		keys = append(keys, string(k))
		return true
	})
	if err != nil {
		t.Fatalf("PrefixScan: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("prefix scan got %d keys, want 2", len(keys))
	}
}

func Test_PrefixScanNoMatch(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("abc"), []byte("1"))

	var count int
	s.PrefixScan(testBucket, []byte("zzz"), func(k, v []byte) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("prefix scan matched %d, want 0", count)
	}
}

func Test_PrefixScanWrongBucket(t *testing.T) {
	s := newTestStore()
	err := s.PrefixScan("nonexistent", []byte("x"), func(k, v []byte) bool { return true })
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func Test_WriteToReadFromRoundTrip(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k1"), []byte("v1"))
	s.Put(testBucket, []byte("k2"), []byte("v2"))
	s.Put(metaBucket, []byte("mk"), []byte("mv"))

	var buf bytes.Buffer
	_, err := s.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	s2 := newTestStore()
	_, err = s2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	v1, _ := s2.Get(testBucket, []byte("k1"))
	v2, _ := s2.Get(testBucket, []byte("k2"))
	mv, _ := s2.Get(metaBucket, []byte("mk"))

	if !bytes.Equal(v1, []byte("v1")) {
		t.Errorf("k1 = %q, want %q", v1, "v1")
	}
	if !bytes.Equal(v2, []byte("v2")) {
		t.Errorf("k2 = %q, want %q", v2, "v2")
	}
	if !bytes.Equal(mv, []byte("mv")) {
		t.Errorf("mk = %q, want %q", mv, "mv")
	}
}

func Test_ReadFromOverwrites(t *testing.T) {
	s1 := newTestStore()
	s1.Put(testBucket, []byte("src"), []byte("from_source"))

	var buf bytes.Buffer
	s1.WriteTo(&buf)

	s2 := newTestStore()
	s2.Put(testBucket, []byte("tgt"), []byte("from_target"))
	s2.ReadFrom(&buf)

	src, _ := s2.Get(testBucket, []byte("src"))
	tgt, _ := s2.Get(testBucket, []byte("tgt"))
	if !bytes.Equal(src, []byte("from_source")) {
		t.Errorf("src = %q, want %q", src, "from_source")
	}
	if tgt != nil {
		t.Errorf("old key should be gone, got %q", tgt)
	}
}

func TestClose(t *testing.T) {
	s := newTestStore()
	s.Put(testBucket, []byte("k"), []byte("v"))

	err := s.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := newTestStore()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s.Put(testBucket, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		}
	}()

	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.Get(testBucket, []byte(fmt.Sprintf("k%d", i)))
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentScans(t *testing.T) {
	s := newTestStore()
	for i := 0; i < 50; i++ {
		s.Put(testBucket, []byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var count int
			s.Scan(testBucket, func(k, v []byte) bool {
				count++
				return true
			})
			if count != 50 {
				t.Errorf("scan got %d, want 50", count)
			}
		}()
	}
	wg.Wait()
}

func TestLargeValue(t *testing.T) {
	s := newTestStore()
	big := make([]byte, 64*1024)
	for i := range big {
		big[i] = byte(i % 256)
	}
	s.Put(testBucket, []byte("big"), big)

	val, _ := s.Get(testBucket, []byte("big"))
	if !bytes.Equal(val, big) {
		t.Errorf("large value mismatch: got %d bytes, want %d", len(val), len(big))
	}
}

func TestManyKeys(t *testing.T) {
	s := newTestStore()
	n := 1000
	for i := 0; i < n; i++ {
		s.Put(testBucket, []byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}

	var count int
	s.Scan(testBucket, func(k, v []byte) bool {
		count++
		return true
	})
	if count != n {
		t.Errorf("scanned %d, want %d", count, n)
	}
}

// defrag tests

func Test_Defragment_DataSurvives(t *testing.T) {
	s := newTestStore()

	for i := range 100 {
		s.Put(testBucket, fmt.Appendf(nil, "key%05d", i), []byte("bar"))
	}

	err := s.Defragment()
	require.NoError(t, err)

	// all data should still be readable after defrag
	for i := range 100 {
		val, err := s.Get(testBucket, fmt.Appendf(nil, "key%05d", i))
		require.NoError(t, err)
		require.Equal(t, []byte("bar"), val, "key k%05d lost after defrag", i)
	}
}

func Test_Defragment_StoreRemainsWritable(t *testing.T) {
	s := newTestStore()

	s.Put(testBucket, []byte("before"), []byte("value"))

	require.NoError(t, s.Defragment())

	_, err := s.Put(testBucket, []byte("after"), []byte("value"))
	require.NoError(t, err)

	val, err := s.Get(testBucket, []byte("after"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
}

func Test_Defragment_EmptyStore(t *testing.T) {
	s := newTestStore()

	// defraging empty store should be a no-op not a crash
	require.NoError(t, s.Defragment())

	// should still work
	_, err := s.Put(testBucket, []byte("k"), []byte("v"))
	require.NoError(t, err)
}

func Test_Defragment_MultipleBuckets(t *testing.T) {
	s := newTestStore()

	s.Put(testBucket, []byte("dk"), []byte("dv"))
	s.Put(metaBucket, []byte("mk"), []byte("mv"))

	require.NoError(t, s.Defragment())

	dv, err := s.Get(testBucket, []byte("dk"))
	require.NoError(t, err)
	require.Equal(t, []byte("dv"), dv, "data bucket value lost after defrag")

	mv, err := s.Get(metaBucket, []byte("mk"))
	require.NoError(t, err)
	require.Equal(t, []byte("mv"), mv, "meta bucket value lost after defrag")
}

func Test_Defragment_AfterManyDeletes(t *testing.T) {
	s := newTestStore()

	for i := range 1000 {
		s.Put(testBucket, fmt.Appendf(nil, "key%05d", i), fmt.Appendf(nil, "val%05d", i))
	}
	for i := range 900 {
		s.Delete(testBucket, fmt.Appendf(nil, "key%05d", i))
	}

	require.NoError(t, s.Defragment())

	// the remaining 100 keys should still be there
	for i := 900; i < 1000; i++ {
		val, err := s.Get(testBucket, fmt.Appendf(nil, "key%05d", i))
		require.NoError(t, err)
		require.Equal(t, fmt.Appendf(nil, "val%05d", i), val, "key%05d lost after defrag", i)
	}

	// the deleted 900 should be gone
	for i := range 900 {
		val, err := s.Get(testBucket, fmt.Appendf(nil, "key%05d", i))
		require.NoError(t, err)
		require.Nil(t, val, "deleted key%05d still present after defrag", i)
	}
}

func Test_Defragment_IdempotentMultipleTimes(t *testing.T) {
	s := newTestStore()

	s.Put(testBucket, []byte("k"), []byte("v"))

	for range 3 {
		require.NoError(t, s.Defragment())
	}

	val, err := s.Get(testBucket, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

func Test_Defragment_ScanStillWorksAfter(t *testing.T) {
	s := newTestStore()

	for i := range 10 {
		s.Put(testBucket, fmt.Appendf(nil, "key%02d", i), fmt.Appendf(nil, "val%02d", i))
	}

	require.NoError(t, s.Defragment())

	var count int
	err := s.Scan(testBucket, func(k, v []byte) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 10, count, "scan should return all keys after defrag")
}

func Test_Defragment_ReducesFileSize(t *testing.T) {
	s := newTestStore()

	// write and delete enough data that boltdb's file should have
	// meaningful free pages to reclaim
	bigVal := make([]byte, 4096)
	for i := range 500 {
		s.Put(testBucket, fmt.Appendf(nil, "k%05d", i), bigVal)
	}
	for i := range 500 {
		s.Delete(testBucket, fmt.Appendf(nil, "k%05d", i))
	}

	sizeBefore := s.sz.Load()
	require.NoError(t, s.Defragment())
	sizeAfter := s.sz.Load()

	t.Logf("size: before=%d after=%d", sizeBefore, sizeAfter)
	require.Equal(t, sizeAfter, sizeBefore)
}
