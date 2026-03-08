package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/balits/kave/internal/storage"
)

const testBucket storage.Bucket = "test"
const metaBucket storage.Bucket = "_meta"

func newTestStore(t *testing.T) *boltStore {
	seed := time.Now().UnixNano()
	tmp := filepath.Join("./testdata", strconv.Itoa(int(seed)), "durable_bytestore_test")
	err := os.MkdirAll(tmp, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(storage.StorageOptions{
		Kind:           storage.StorageKindBoltdb,
		Dir:            tmp,
		InitialBuckets: []storage.Bucket{"test", "_meta"},
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s.(*boltStore)
}

func TestGetMissingKey(t *testing.T) {
	s := newTestStore(t)
	val, err := s.Get(testBucket, []byte("nope"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %q", val)
	}
}

func TestGetWrongBucket(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("nonexistent", []byte("key"))
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func TestGetAfterPut(t *testing.T) {
	s := newTestStore(t)
	s.Put(testBucket, []byte("k"), []byte("v"))

	val, err := s.Get(testBucket, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("v")) {
		t.Errorf("Get = %q, want %q", val, "v")
	}
}

func TestPutWrongBucket(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Put("nonexistent", []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := newTestStore(t)
	s.Put(testBucket, []byte("k"), []byte("v1"))
	s.Put(testBucket, []byte("k"), []byte("v2"))

	val, _ := s.Get(testBucket, []byte("k"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("overwrite: got %q, want %q", val, "v2")
	}
}

func TestPutMultipleBuckets(t *testing.T) {
	s := newTestStore(t)
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

func TestDeleteExisting(t *testing.T) {
	s := newTestStore(t)
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

func TestDeleteMissing(t *testing.T) {
	s := newTestStore(t)
	val, err := s.Delete(testBucket, []byte("nope"))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for missing key, got %q", val)
	}
}

func TestDeleteWrongBucket(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Delete("nonexistent", []byte("k"))
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func TestScanAll(t *testing.T) {
	s := newTestStore(t)
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

func TestScanEarlyStop(t *testing.T) {
	s := newTestStore(t)
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

func TestScanWrongBucket(t *testing.T) {
	s := newTestStore(t)
	err := s.Scan("nonexistent", func(k, v []byte) bool { return true })
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func TestScanOrder(t *testing.T) {
	s := newTestStore(t)
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

func TestPrefixScan(t *testing.T) {
	s := newTestStore(t)
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

func TestPrefixScanNoMatch(t *testing.T) {
	s := newTestStore(t)
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

func TestPrefixScanWrongBucket(t *testing.T) {
	s := newTestStore(t)
	err := s.PrefixScan("nonexistent", []byte("x"), func(k, v []byte) bool { return true })
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

func TestWriteToReadFromRoundTrip(t *testing.T) {
	s := newTestStore(t)
	s.Put(testBucket, []byte("k1"), []byte("v1"))
	s.Put(testBucket, []byte("k2"), []byte("v2"))
	s.Put(metaBucket, []byte("mk"), []byte("mv"))

	var buf bytes.Buffer
	_, err := s.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	s2 := newTestStore(t)

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

func TestReadFromOverwrites(t *testing.T) {
	s1 := newTestStore(t)
	s1.Put(testBucket, []byte("src"), []byte("from_source"))

	var buf bytes.Buffer
	s1.WriteTo(&buf)

	s2 := newTestStore(t)
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
	s := newTestStore(t)
	s.Put(testBucket, []byte("k"), []byte("v"))

	err := s.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := newTestStore(t)
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
	s := newTestStore(t)
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
	s := newTestStore(t)
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
	s := newTestStore(t)
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

func Test_NoTest_JustRemoving_Testdata_files(t *testing.T) {
	err := os.RemoveAll("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Mkdir("./testdata", 0o755)
	if err != nil {
		t.Fatal(err)
	}
}
