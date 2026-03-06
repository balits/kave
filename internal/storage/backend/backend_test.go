package backend

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/balits/kave/internal/storage"
)

const (
	testBucket storage.Bucket = "test"
	metaBucket storage.Bucket = "_meta"
)

func newTestBackend(t *testing.T, kind storage.StorageKind) Backend {
	t.Helper()

	opts := storage.StorageOptions{
		Kind:           kind,
		Dir:            t.TempDir(),
		InitialBuckets: []storage.Bucket{testBucket, metaBucket},
	}

	switch kind {
	case storage.StorageKindInMemory, storage.StorageKindBoltdb:
		//t.Skip("boltdb tests require disk setup — skipping")
		return NewBackend(opts)
	default:
		t.Fatalf("unknown storage kind: %s", kind)
		return nil
	}
}

func runForAllKinds(t *testing.T, name string, f func(t *testing.T, kind storage.StorageKind)) {
	t.Helper()
	kinds := []storage.StorageKind{storage.StorageKindInMemory}
	for _, kind := range kinds {
		t.Run(fmt.Sprintf("%s/%s", name, kind), func(t *testing.T) {
			f(t, kind)
		})
	}
}

func Test_NewBackend(t *testing.T) {
	runForAllKinds(t, "NewBackend", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		if b == nil {
			t.Fatal("NewBackend returned nil")
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

func Test_ReadTxUnsafeGetEmpty(t *testing.T) {
	runForAllKinds(t, "UnsafeGetEmpty", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, err := rtx.UnsafeGet(testBucket, []byte("missing"))
		if err != nil {
			t.Fatalf("UnsafeGet: %v", err)
		}
		if val != nil {
			t.Errorf("expected nil for missing key, got %v", val)
		}
	})
}

func Test_ReadTxUnsafeGetAfterWrite(t *testing.T) {
	runForAllKinds(t, "UnsafeGetAfterWrite", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("key1"), []byte("val1"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, err := rtx.UnsafeGet(testBucket, []byte("key1"))
		if err != nil {
			t.Fatalf("UnsafeGet: %v", err)
		}
		if !bytes.Equal(val, []byte("val1")) {
			t.Errorf("UnsafeGet = %q, want %q", val, "val1")
		}
	})
}

func Test_ReadTxUnsafeGetWrongBucket(t *testing.T) {
	runForAllKinds(t, "UnsafeGetWrongBucket", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		_, err := rtx.UnsafeGet(storage.Bucket("nonexistent"), []byte("key"))
		if err == nil {
			t.Error("expected error for non-existent bucket")
		}
	})
}

func Test_ReadTxUnsafeScan(t *testing.T) {
	runForAllKinds(t, "UnsafeScan", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("a"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("b"), []byte("2"))
		wtx.UnsafePut(testBucket, []byte("c"), []byte("3"))
		wtx.UnsafePut(testBucket, []byte("d"), []byte("4"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		var scanned []string
		err := rtx.UnsafeScan(testBucket, []byte("b"), []byte("d"), func(k, v []byte) error {
			scanned = append(scanned, string(k))
			return nil
		})
		if err != nil {
			t.Fatalf("UnsafeScan: %v", err)
		}
		if len(scanned) != 2 || scanned[0] != "b" || scanned[1] != "c" {
			t.Errorf("UnsafeScan = %v, want [b c]", scanned)
		}
	})
}

func Test_ReadTxUnsafeScanAll(t *testing.T) {
	runForAllKinds(t, "UnsafeScanAll", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("x"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("y"), []byte("2"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		var count int
		err := rtx.UnsafeScan(testBucket, nil, nil, func(k, v []byte) error {
			count++
			return nil
		})
		if err != nil {
			t.Fatalf("UnsafeScan: %v", err)
		}
		if count != 2 {
			t.Errorf("scanned %d keys, want 2", count)
		}
	})
}

func Test_ReadTxUnsafeScanCallbackError(t *testing.T) {
	runForAllKinds(t, "UnsafeScanCallbackError", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("a"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("b"), []byte("2"))
		wtx.UnsafePut(testBucket, []byte("c"), []byte("3"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		callbackErr := errors.New("stop scanning")
		var visited int
		err := rtx.UnsafeScan(testBucket, nil, nil, func(k, v []byte) error {
			visited++
			if visited >= 2 {
				return callbackErr
			}
			return nil
		})
		if err == nil {
			t.Error("expected callback error to propagate")
		}
		if visited > 2 {
			t.Errorf("visited %d keys after error, expected scan to stop at 2", visited)
		}
	})
}

func Test_ReadTxUnsafeRange(t *testing.T) {
	runForAllKinds(t, "UnsafeRange", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("a"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("b"), []byte("2"))
		wtx.UnsafePut(testBucket, []byte("c"), []byte("3"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		results, err := rtx.UnsafeRange(testBucket, []byte("a"), []byte("c"), func(k, v []byte) error {
			return nil
		})
		if err != nil {
			t.Fatalf("UnsafeRange: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("UnsafeRange returned %d results, want 2", len(results))
		}
	})
}


func Test_WriteTxPutAndCommit(t *testing.T) {
	runForAllKinds(t, "PutAndCommit", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		if err := wtx.UnsafePut(testBucket, []byte("k"), []byte("v")); err != nil {
			t.Fatalf("UnsafePut: %v", err)
		}
		if err := wtx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})
}

func Test_WriteTxPutAndCommitThenRead(t *testing.T) {
	runForAllKinds(t, "PutCommitRead", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("k"), []byte("v"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, _ := rtx.UnsafeGet(testBucket, []byte("k"))
		if !bytes.Equal(val, []byte("v")) {
			t.Errorf("after commit: val = %q, want %q", val, "v")
		}
	})
}

func Test_WriteTxAbort(t *testing.T) {
	runForAllKinds(t, "Abort", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("existing"), []byte("yes"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("aborted"), []byte("no"))
		wtx.Abort()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, _ := rtx.UnsafeGet(testBucket, []byte("aborted"))
		existing, _ := rtx.UnsafeGet(testBucket, []byte("existing"))
		if val != nil {
			t.Errorf("aborted key should not exist, got %q", val)
		}
		if !bytes.Equal(existing, []byte("yes")) {
			t.Error("existing key should still be present after abort")
		}
	})
}

func Test_WriteTxDelete(t *testing.T) {
	runForAllKinds(t, "Delete", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("del_me"), []byte("val"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		if err := wtx.UnsafeDelete(testBucket, []byte("del_me")); err != nil {
			t.Fatalf("UnsafeDelete: %v", err)
		}
		wtx.Commit()
	})
}

func Test_WriteTxDeleteVerify(t *testing.T) {
	runForAllKinds(t, "DeleteVerify", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("del_me"), []byte("val"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		wtx.UnsafeDelete(testBucket, []byte("del_me"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, _ := rtx.UnsafeGet(testBucket, []byte("del_me"))
		if val != nil {
			t.Errorf("deleted key still present: %q", val)
		}
	})
}

func Test_WriteTxDeleteNonExistent(t *testing.T) {
	runForAllKinds(t, "DeleteNonExistent", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		err := wtx.UnsafeDelete(testBucket, []byte("nope"))
		if err != nil {
			t.Fatalf("UnsafeDelete on missing key: %v", err)
		}
		wtx.Commit()
	})
}

func Test_WriteTxMultipleOps(t *testing.T) {
	runForAllKinds(t, "MultipleOps", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("a"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("b"), []byte("2"))
		wtx.UnsafePut(testBucket, []byte("c"), []byte("3"))
		wtx.UnsafeDelete(testBucket, []byte("b"))
		wtx.UnsafePut(testBucket, []byte("a"), []byte("updated"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		aVal, _ := rtx.UnsafeGet(testBucket, []byte("a"))
		bVal, _ := rtx.UnsafeGet(testBucket, []byte("b"))
		cVal, _ := rtx.UnsafeGet(testBucket, []byte("c"))
		if !bytes.Equal(aVal, []byte("updated")) {
			t.Errorf("a = %q, want %q", aVal, "updated")
		}
		if bVal != nil {
			t.Errorf("b should be deleted, got %q", bVal)
		}
		if !bytes.Equal(cVal, []byte("3")) {
			t.Errorf("c = %q, want %q", cVal, "3")
		}
	})
}

func Test_WriteTxMultipleBuckets(t *testing.T) {
	runForAllKinds(t, "MultipleBuckets", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("data_key"), []byte("data_val"))
		wtx.UnsafePut(metaBucket, []byte("meta_key"), []byte("meta_val"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		dv, _ := rtx.UnsafeGet(testBucket, []byte("data_key"))
		mv, _ := rtx.UnsafeGet(metaBucket, []byte("meta_key"))
		if !bytes.Equal(dv, []byte("data_val")) {
			t.Errorf("data bucket: %q, want %q", dv, "data_val")
		}
		if !bytes.Equal(mv, []byte("meta_val")) {
			t.Errorf("meta bucket: %q, want %q", mv, "meta_val")
		}
	})
}

func Test_WriteTxOverwriteValue(t *testing.T) {
	runForAllKinds(t, "OverwriteValue", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("k"), []byte("v1"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("k"), []byte("v2"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, _ := rtx.UnsafeGet(testBucket, []byte("k"))
		if !bytes.Equal(val, []byte("v2")) {
			t.Errorf("overwrite: val = %q, want %q", val, "v2")
		}
	})
}


func Test_WriteTxCanRead(t *testing.T) {
	runForAllKinds(t, "WriteTxCanRead", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("rk"), []byte("rv"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		val, err := wtx.UnsafeGet(testBucket, []byte("rk"))
		if err != nil {
			t.Fatalf("WriteTx.UnsafeGet: %v", err)
		}
		if !bytes.Equal(val, []byte("rv")) {
			t.Errorf("WriteTx read: val = %q, want %q", val, "rv")
		}
		wtx.Abort()
	})
}

func Test_WriteTxUnsafeScan(t *testing.T) {
	runForAllKinds(t, "WriteTxUnsafeScan", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("p"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("q"), []byte("2"))
		wtx.UnsafePut(testBucket, []byte("r"), []byte("3"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		var keys []string
		err := wtx.UnsafeScan(testBucket, []byte("p"), []byte("r"), func(k, v []byte) error {
			keys = append(keys, string(k))
			return nil
		})
		wtx.Abort()

		if err != nil {
			t.Fatalf("WriteTx.UnsafeScan: %v", err)
		}
		if len(keys) != 2 || keys[0] != "p" || keys[1] != "q" {
			t.Errorf("WriteTx scan = %v, want [p q]", keys)
		}
	})
}


func Test_BackendSnapshotAndRestore(t *testing.T) {
	runForAllKinds(t, "SnapshotAndRestore", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("snap_k1"), []byte("snap_v1"))
		wtx.UnsafePut(testBucket, []byte("snap_k2"), []byte("snap_v2"))
		wtx.UnsafePut(metaBucket, []byte("meta_k"), []byte("meta_v"))
		wtx.Commit()
		wtx.Unlock()

		var buf bytes.Buffer
		if err := b.Snapshot(&buf); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if buf.Len() == 0 {
			t.Fatal("Snapshot produced empty buffer")
		}

		b2 := newTestBackend(t, kind)
		defer b2.Close()

		if err := b2.Restore(&buf); err != nil {
			t.Fatalf("Restore: %v", err)
		}

		rtx := b2.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		v1, _ := rtx.UnsafeGet(testBucket, []byte("snap_k1"))
		v2, _ := rtx.UnsafeGet(testBucket, []byte("snap_k2"))
		mv, _ := rtx.UnsafeGet(metaBucket, []byte("meta_k"))
		if !bytes.Equal(v1, []byte("snap_v1")) {
			t.Errorf("restored snap_k1 = %q, want %q", v1, "snap_v1")
		}
		if !bytes.Equal(v2, []byte("snap_v2")) {
			t.Errorf("restored snap_k2 = %q, want %q", v2, "snap_v2")
		}
		if !bytes.Equal(mv, []byte("meta_v")) {
			t.Errorf("restored meta_k = %q, want %q", mv, "meta_v")
		}
	})
}

func Test_BackendRestoreOverwritesExisting(t *testing.T) {
	runForAllKinds(t, "RestoreOverwrites", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("src"), []byte("from_source"))
		wtx.Commit()
		wtx.Unlock()

		var buf bytes.Buffer
		b.Snapshot(&buf)

		b2 := newTestBackend(t, kind)
		defer b2.Close()

		wtx = b2.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("tgt"), []byte("from_target"))
		wtx.Commit()
		wtx.Unlock()

		b2.Restore(&buf)

		rtx := b2.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		srcVal, _ := rtx.UnsafeGet(testBucket, []byte("src"))
		tgtVal, _ := rtx.UnsafeGet(testBucket, []byte("tgt"))
		if !bytes.Equal(srcVal, []byte("from_source")) {
			t.Errorf("restored src = %q, want %q", srcVal, "from_source")
		}
		if tgtVal != nil {
			t.Errorf("old target key should be gone after restore, got %q", tgtVal)
		}
	})
}


func Test_BackendForceCommitNoPendingBatch(t *testing.T) {
	runForAllKinds(t, "ForceCommitNoBatch", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		if err := b.ForceCommit(); err != nil {
			t.Fatalf("ForceCommit with no batch: %v", err)
		}
	})
}

func Test_BackendCommitNoPendingBatch(t *testing.T) {
	runForAllKinds(t, "CommitNoBatch", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		if err := b.Commit(); err != nil {
			t.Fatalf("Commit with no batch: %v", err)
		}
	})
}


func Test_BackendConcurrentReads(t *testing.T) {
	runForAllKinds(t, "ConcurrentReads", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key_%03d", i))
			val := []byte(fmt.Sprintf("val_%03d", i))
			wtx.UnsafePut(testBucket, key, val)
		}
		wtx.Commit()
		wtx.Unlock()

		var wg sync.WaitGroup
		errCh := make(chan error, 10)

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					rtx := b.ReadTx()
					rtx.RLock()
					key := []byte(fmt.Sprintf("key_%03d", i))
					val, err := rtx.UnsafeGet(testBucket, key)
					rtx.RUnlock()

					if err != nil {
						errCh <- err
						return
					}
					expected := []byte(fmt.Sprintf("val_%03d", i))
					if !bytes.Equal(val, expected) {
						errCh <- fmt.Errorf("value mismatch for %s", key)
						return
					}
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			t.Errorf("concurrent read error: %v", err)
		}
	})
}

func Test_BackendWriteThenReadConcurrent(t *testing.T) {
	runForAllKinds(t, "WriteThenReadConcurrent", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				wtx := b.WriteTx()
				wtx.Lock()
				key := fmt.Appendf(nil, "ckey_%03d", i)
				val := fmt.Appendf(nil, "cval_%03d", i)
				wtx.UnsafePut(testBucket, key, val)
				wtx.Commit()
				wtx.Unlock()
			}
		}()

		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 100 {
					rtx := b.ReadTx()
					rtx.RLock()
					rtx.UnsafeScan(testBucket, nil, nil, func(k, v []byte) error {
						return nil
					})
					rtx.RUnlock()
				}
			}()
		}

		wg.Wait()
	})
}


func Test_WriteTxRejectsEmptyKey(t *testing.T) {
	runForAllKinds(t, "RejectsEmptyKey", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		err := wtx.UnsafePut(testBucket, []byte(""), []byte("val"))
		if err == nil {
			t.Fatal("expected error for empty key, got nil")
		}
		wtx.Abort()
	})
}

func Test_WriteTxAllowsEmptyValue(t *testing.T) {
	runForAllKinds(t, "AllowsEmptyValue", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		defer wtx.Unlock()

		err := wtx.UnsafePut(testBucket, []byte("k"), []byte(""))
		if err != nil {
			t.Fatalf("empty value should be allowed: %v", err)
		}
		wtx.Commit()
	})
}

func Test_WriteTxLargeValue(t *testing.T) {
	runForAllKinds(t, "LargeValue", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		bigVal := make([]byte, 64*1024)
		for i := range bigVal {
			bigVal[i] = byte(i % 256)
		}

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("big"), bigVal)
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, _ := rtx.UnsafeGet(testBucket, []byte("big"))
		if !bytes.Equal(val, bigVal) {
			t.Errorf("large value mismatch: got %d bytes, want %d", len(val), len(bigVal))
		}
	})
}

func Test_WriteTxManyKeys(t *testing.T) {
	runForAllKinds(t, "ManyKeys", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		n := 1000
		wtx := b.WriteTx()
		wtx.Lock()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("batch_key_%05d", i))
			val := []byte(fmt.Sprintf("batch_val_%05d", i))
			wtx.UnsafePut(testBucket, key, val)
		}
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		count := 0
		rtx.UnsafeScan(testBucket, nil, nil, func(k, v []byte) error {
			count++
			return nil
		})
		if count != n {
			t.Errorf("scanned %d keys, want %d", count, n)
		}
	})
}


func Test_ScanRangesBoundaryConditions(t *testing.T) {
	runForAllKinds(t, "ScanBoundaries", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("aa"), []byte("1"))
		wtx.UnsafePut(testBucket, []byte("ab"), []byte("2"))
		wtx.UnsafePut(testBucket, []byte("ba"), []byte("3"))
		wtx.UnsafePut(testBucket, []byte("bb"), []byte("4"))
		wtx.Commit()
		wtx.Unlock()

		tests := []struct {
			name       string
			start, end []byte
			wantKeys   []string
		}{
			{"exact range", []byte("ab"), []byte("ba"), []string{"ab"}},
			{"start inclusive", []byte("aa"), []byte("ab"), []string{"aa"}},
			{"full range", []byte("aa"), []byte("bc"), []string{"aa", "ab", "ba", "bb"}},
			{"empty range", []byte("ca"), []byte("cb"), nil},
			{"single key match", []byte("ba"), []byte("bb"), []string{"ba"}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rtx := b.ReadTx()
				rtx.RLock()
				defer rtx.RUnlock()

				var got []string
				rtx.UnsafeScan(testBucket, tt.start, tt.end, func(k, v []byte) error {
					got = append(got, string(k))
					return nil
				})
				if len(got) != len(tt.wantKeys) {
					t.Errorf("got %v, want %v", got, tt.wantKeys)
					return
				}
				for i := range got {
					if got[i] != tt.wantKeys[i] {
						t.Errorf("key[%d] = %q, want %q", i, got[i], tt.wantKeys[i])
					}
				}
			})
		}
	})
}


func Test_SequentialWriteTransactions(t *testing.T) {
	runForAllKinds(t, "SequentialTxns", func(t *testing.T, kind storage.StorageKind) {
		b := newTestBackend(t, kind)
		defer b.Close()

		wtx := b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("seq"), []byte("v1"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("seq"), []byte("v2"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		wtx.UnsafeDelete(testBucket, []byte("seq"))
		wtx.Commit()
		wtx.Unlock()

		wtx = b.WriteTx()
		wtx.Lock()
		wtx.UnsafePut(testBucket, []byte("seq"), []byte("v3"))
		wtx.Commit()
		wtx.Unlock()

		rtx := b.ReadTx()
		rtx.RLock()
		defer rtx.RUnlock()

		val, _ := rtx.UnsafeGet(testBucket, []byte("seq"))
		if !bytes.Equal(val, []byte("v3")) {
			t.Errorf("after sequential txns: val = %q, want %q", val, "v3")
		}
	})
}
