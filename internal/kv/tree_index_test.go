package kv

import (
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func Test_TreeIndexPutAndGet(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())

	if err := ti.Put([]byte("foo"), Revision{1, 0}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	createRev, modRev, ver, err := ti.Get([]byte("foo"), 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if createRev != (Revision{1, 0}) || modRev != (Revision{1, 0}) || ver != 1 {
		t.Errorf("Get = rev:%v created:%v ver:%d, want {1,0} {1,0} 1", modRev, createRev, ver)
	}

	// Update the key
	ti.Put([]byte("foo"), Revision{2, 0})
	createRev, modRev, ver, err = ti.Get([]byte("foo"), 2)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if createRev != (Revision{1, 0}) || modRev != (Revision{2, 0}) || ver != 2 {
		t.Errorf("Get = rev:%v created:%v ver:%d, want {2,0} {1,0} 2", modRev, createRev, ver)
	}

	// Reading at old revision still returns old value
	_, modRev, ver, err = ti.Get([]byte("foo"), 1)
	if err != nil {
		t.Fatalf("Get at old rev: %v", err)
	}
	if modRev != (Revision{1, 0}) || ver != 1 {
		t.Errorf("Get(rev=1) = rev:%v ver:%d, want {1,0} 1", modRev, ver)
	}
}

func Test_TreeIndexGetNonExistent(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	_, _, _, err := ti.Get([]byte("missing"), 1)
	if err == nil {
		t.Error("expected error for non-existent key")
	}
}

func Test_TreeIndexMultipleKeys(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("a"), Revision{1, 0})
	ti.Put([]byte("b"), Revision{2, 0})
	ti.Put([]byte("c"), Revision{3, 0})

	for _, tc := range []struct {
		key  string
		rev  int64
		main int64
	}{
		{"a", 5, 1}, {"b", 5, 2}, {"c", 5, 3},
	} {
		_, modRev, _, err := ti.Get([]byte(tc.key), tc.rev)
		if err != nil {
			t.Fatalf("Get(%q): %v", tc.key, err)
		}
		if modRev.Main != tc.main {
			t.Errorf("Get(%q).Main = %d, want %d", tc.key, modRev.Main, tc.main)
		}
	}
}

func Test_TreeIndexTombstone(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("foo"), Revision{1, 0})

	if err := ti.Tombstone([]byte("foo"), Revision{2, 0}); err != nil {
		t.Fatalf("Tombstone: %v", err)
	}

	// Key should not be found at or after tombstone revision
	_, _, _, err := ti.Get([]byte("foo"), 2)
	if err == nil {
		t.Error("expected error for tombstoned key at tombstone rev")
	}
	_, _, _, err = ti.Get([]byte("foo"), 3)
	if err == nil {
		t.Error("expected error for tombstoned key after tombstone rev")
	}

	// Key should still be found before tombstone
	_, _, _, err = ti.Get([]byte("foo"), 1)
	if err != nil {
		t.Errorf("Get before tombstone should work: %v", err)
	}

	// Tombstoning a non-existent key should error
	err = ti.Tombstone([]byte("bar"), Revision{3, 0})
	if err == nil {
		t.Error("expected error for tombstoning non-existent key")
	}

	// Tombstoning an already-tombstoned key (empty gen) should error
	err = ti.Tombstone([]byte("foo"), Revision{3, 0})
	if err == nil {
		t.Error("expected error for double-tombstoning")
	}
}

func Test_TreeIndexRange(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("bar"), Revision{1, 0})
	ti.Put([]byte("baz"), Revision{2, 0})
	ti.Put([]byte("foo"), Revision{3, 0})
	ti.Put([]byte("foo1"), Revision{4, 0})

	// Range [bar, foo1) at rev 5
	keys, revs := ti.Range([]byte("bar"), []byte("foo1"), 5)
	if len(keys) != 3 {
		t.Fatalf("Range returned %d keys, want 3", len(keys))
	}
	wantKeys := []string{"bar", "baz", "foo"}
	for i, k := range keys {
		if string(k) != wantKeys[i] {
			t.Errorf("keys[%d] = %q, want %q", i, k, wantKeys[i])
		}
	}
	_ = revs

	// Point query (end=nil)
	keys, _ = ti.Range([]byte("bar"), nil, 5)
	if len(keys) != 1 || string(keys[0]) != "bar" {
		t.Errorf("point query = %v, want [bar]", keys)
	}

	// Range returns nothing if key doesn't exist
	keys, _ = ti.Range([]byte("zzz"), nil, 5)
	if len(keys) != 0 {
		t.Errorf("non-existent point query = %v, want empty", keys)
	}
}

func Test_TreeIndexRevisions(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("foo"), Revision{1, 0})
	ti.Put([]byte("foo1"), Revision{2, 0})
	ti.Put([]byte("foo2"), Revision{3, 0})
	ti.Put([]byte("foo"), Revision{4, 0})

	// All keys in [foo, foo3) at rev 4
	revs, total := ti.Revisions([]byte("foo"), []byte("foo3"), 4, 0)
	t.Log(revs)
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if len(revs) != 3 {
		t.Errorf("revs = %d, want 3", len(revs))
	}

	// With limit
	revs, total = ti.Revisions([]byte("foo"), []byte("foo3"), 4, 2)
	t.Log(revs)
	if total != 3 {
		t.Errorf("total with limit = %d, want 3", total)
	}
	if len(revs) != 2 {
		t.Errorf("limited revs = %d, want 2", len(revs))
	}

	// Point query
	revs, total = ti.Revisions([]byte("foo"), nil, 4, 0)
	t.Log(revs)
	if total != 1 || len(revs) != 1 {
		t.Errorf("point revision = %v total=%d, want 1 rev", revs, total)
	}
	if revs[0] != (Revision{4, 0}) {
		t.Errorf("point rev = %v, want {4,0}", revs[0])
	}
}

func Test_TreeIndexCountRevisions(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("a"), Revision{1, 0})
	ti.Put([]byte("b"), Revision{2, 0})
	ti.Put([]byte("c"), Revision{3, 0})
	ti.Tombstone([]byte("b"), Revision{4, 0})

	// All alive at rev 3
	count := ti.CountRevisions([]byte("a"), []byte("d"), 3)
	if count != 3 {
		t.Errorf("count at rev 3 = %d, want 3", count)
	}

	// At rev 4, "b" is tombstoned
	count = ti.CountRevisions([]byte("a"), []byte("d"), 4)
	if count != 2 {
		t.Errorf("count at rev 4 = %d, want 2", count)
	}

	// Point query
	count = ti.CountRevisions([]byte("a"), nil, 4)
	if count != 1 {
		t.Errorf("point count = %d, want 1", count)
	}
}

func Test_TreeIndexCompact(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("foo"), Revision{1, 0})
	ti.Put([]byte("foo"), Revision{2, 0})
	ti.Put([]byte("bar"), Revision{3, 0})
	ti.Tombstone([]byte("foo"), Revision{4, 0})

	avail, err := ti.Compact(4)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// "bar" at {3,0} should be kept, "foo" tombstone at {4,0} should be kept
	if _, ok := avail[Revision{3, 0}]; !ok {
		t.Error("compact should keep {3,0} for bar")
	}
	if _, ok := avail[Revision{4, 0}]; !ok {
		t.Error("compact should keep {4,0} for foo tombstone")
	}
	// {1,0} and {2,0} should NOT be kept
	if _, ok := avail[Revision{1, 0}]; ok {
		t.Error("compact should NOT keep superseded {1,0}")
	}
	if _, ok := avail[Revision{2, 0}]; ok {
		t.Error("compact should NOT keep superseded {2,0}")
	}
}

func Test_TreeIndexCompactRemovesEmptyKeys(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("foo"), Revision{1, 0})
	ti.Tombstone([]byte("foo"), Revision{2, 0})

	ti.Compact(2)

	// After compacting past the tombstone, key should be gone
	_, _, _, err := ti.Get([]byte("foo"), 100)
	if err == nil {
		t.Error("key should be fully compacted away")
	}
}

func Test_TreeIndexKeep(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("a"), Revision{1, 0})
	ti.Put([]byte("a"), Revision{3, 0})
	ti.Put([]byte("b"), Revision{2, 0})

	avail := ti.Keep(2)
	if _, ok := avail[Revision{1, 0}]; !ok {
		t.Error("keep(2) should include {1,0} for key 'a'")
	}
	if _, ok := avail[Revision{2, 0}]; !ok {
		t.Error("keep(2) should include {2,0} for key 'b'")
	}
}

func Test_TreeIndexClear(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())
	ti.Put([]byte("foo"), Revision{1, 0})
	ti.Put([]byte("bar"), Revision{2, 0})

	ti.Clear()

	_, _, _, err := ti.Get([]byte("foo"), 1)
	if err == nil {
		t.Error("Get after Clear should error")
	}
}

func Test_TreeIndexEqual(t *testing.T) {
	t.Parallel()
	a := NewTreeIndex(testLogger())
	b := NewTreeIndex(testLogger())

	a.Put([]byte("foo"), Revision{1, 0})
	b.Put([]byte("foo"), Revision{1, 0})

	if !a.Equal(b) {
		t.Error("identical indexes should be equal")
	}

	a.Put([]byte("bar"), Revision{2, 0})
	if a.Equal(b) {
		t.Error("different indexes should not be equal")
	}
}

func Test_TreeIndexInsertAndKeyIndex(t *testing.T) {
	t.Parallel()
	ti := NewTreeIndex(testLogger())

	ki := &keyIndex{key: []byte("manual")}
	ki.put(5, 0)
	ti.Insert(ki)

	got := ti.KeyIndex(&keyIndex{key: []byte("manual")})
	if got == nil {
		t.Fatal("KeyIndex returned nil after Insert")
	}
	if !reflect.DeepEqual(got, ki) {
		t.Errorf("KeyIndex = %+v, want %+v", got, ki)
	}

	// Non-existent key
	got = ti.KeyIndex(&keyIndex{key: []byte("missing")})
	if got != nil {
		t.Errorf("KeyIndex for missing key = %+v, want nil", got)
	}
}

func Test_TreeIndex_RevisionRange(t *testing.T) {
	ti := NewTreeIndex(slog.Default())

	ti.Put([]byte("p"), Revision{Main: 2})
	ti.Put([]byte("p"), Revision{Main: 4})
	ti.Put([]byte("p"), Revision{Main: 6})

	// p, 0-9
	got := ti.RevisionsRange([]byte("p"), nil, 0, 10)
	t.Log(got)
	require.Len(t, got, 3)
	require.Equal(t, int64(2), got[0].Main)
	require.Equal(t, int64(4), got[1].Main)
	require.Equal(t, int64(6), got[2].Main)

	ti.Put([]byte("q"), Revision{Main: 8})

	// p-q, 0-9
	got = ti.RevisionsRange([]byte("p"), []byte("r"), 0, 10)
	t.Log(got)
	require.Len(t, got, 4)
	require.Equal(t, int64(2), got[0].Main)
	require.Equal(t, int64(4), got[1].Main)
	require.Equal(t, int64(6), got[2].Main)
	require.Equal(t, int64(8), got[3].Main)

	// a-z, 0-9
	got = ti.RevisionsRange(nil, nil, 0, 10)
	t.Log(got)
	require.Len(t, got, 4)

	// a-z, 0-8
	got = ti.RevisionsRange(nil, nil, 0, 9)
	t.Log(got)
	require.Len(t, got, 4)

	// a-z, 0-7
	got = ti.RevisionsRange(nil, nil, 0, 8)
	t.Log(got)
	require.Len(t, got, 3)

	ti.Put([]byte("r"), Revision{Main: 10})

	// a-s, 0-9
	got = ti.RevisionsRange(nil, []byte("s"), 0, 10)
	t.Log(got)
	require.Len(t, got, 4)

	// a-s, 0-10
	got = ti.RevisionsRange(nil, []byte("s"), 0, 11)
	t.Log(got)
	require.Len(t, got, 5)

}

func Test_TreeIndex_RevisionRange_Idk(t *testing.T) {
	ti := NewTreeIndex(slog.Default())

	ti.Put([]byte("a"), Revision{Main: 1})
	ti.Tombstone([]byte("a"), Revision{Main: 2})
	ti.Put([]byte("a"), Revision{Main: 3})

	ti.Put([]byte("b"), Revision{Main: 4})

	got := ti.RevisionsRange([]byte("a"), nil, 0, 3+1)
	t.Log(got)
	require.Len(t, got, 3)
	require.Equal(t, int64(1), got[0].Main)
	require.Equal(t, int64(2), got[1].Main)
	require.Equal(t, int64(3), got[2].Main)
}
