package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types/api"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type testKVService struct {
	KVService
	lm  *lease.LeaseManager
	t   *testing.T
	ctx context.Context
}

func newTestKVService(t *testing.T) *testKVService {
	t.Helper()
	me := config.Peer{
		NodeID: "test",
	}
	logger := slog.Default()
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	kvstore := mvcc.NewKVStore(reg, logger, backend)
	lm := lease.NewManager(reg, logger, kvstore, backend)
	t.Cleanup(func() { backend.Close() })
	fsm := fsm.New(logger, kvstore, lm, me.NodeID)

	isLeader := func() bool { return true }
	var logIndex atomic.Uint64
	propose := func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		bs, err := command.Encode(cmd)
		if err != nil {
			return nil, err
		}
		idx := logIndex.Add(1)
		res := fsm.Apply(&raft.Log{
			Index: idx,
			Data:  bs,
			Term:  1,
			Type:  raft.LogCommand,
		})
		result, ok := res.(command.Result)
		if !ok {
			return nil, fmt.Errorf("unexpected result type from FSM")
		}
		return &result, nil
	}

	peersvc := &mockPeerService{me: me, isLeader: isLeader}
	svc := NewKVService(logger, kvstore, peersvc, propose)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	return &testKVService{
		KVService: svc,
		lm:        lm,
		t:         t,
		ctx:       ctx,
	}
}

// mustPut inserts a key-value pair and asserts no error.
func (ts *testKVService) mustPut(key, value string) *api.KvPutResponse {
	ts.t.Helper()
	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:   []byte(key),
		Value: []byte(value),
	})
	require.NoError(ts.t, err, "Put(%q, %q) failed", key, value)
	require.NotNil(ts.t, result)
	return result
}

// mustPutWithPrev inserts a key-value pair with PrevEntry=true and asserts no error.
func (ts *testKVService) mustPutWithPrev(key, value string) *api.KvPutResponse {
	ts.t.Helper()
	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:       []byte(key),
		Value:     []byte(value),
		PrevEntry: true,
	})
	require.NoError(ts.t, err, "Put(%q, %q, prev=true) failed", key, value)
	require.NotNil(ts.t, result)
	return result
}

// mustRange performs a range query and asserts no error.
func (ts *testKVService) mustRange(req api.KvRangeRequest) *api.KvRangeResponse {
	ts.t.Helper()
	result, err := ts.Range(ts.ctx, req)
	require.NoError(ts.t, err, "Range failed")
	require.NotNil(ts.t, result)
	return result
}

// mustDelete performs a delete and asserts no error.
func (ts *testKVService) mustDelete(key string, end string, prevEntries bool) *api.KvDeleteResponse {
	ts.t.Helper()
	cmd := command.DeleteCmd{
		Key:         []byte(key),
		PrevEntries: prevEntries,
	}
	if end != "" {
		cmd.End = []byte(end)
	}
	result, err := ts.Delete(ts.ctx, cmd)
	require.NoError(ts.t, err, "Delete(%q) failed", key)
	require.NotNil(ts.t, result)
	return result
}

func Test_KVService_Put_Single(t *testing.T) {
	ts := newTestKVService(t)

	result := ts.mustPut("foo", "bar")

	require.Equal(t, int64(1), result.Header.Revision)
	require.Nil(t, result.PrevEntry, "PrevEntry should be nil when not requested")
}

func Test_KVService_Put_Multiple(t *testing.T) {
	ts := newTestKVService(t)

	r1 := ts.mustPut("a", "1")
	r2 := ts.mustPut("b", "2")
	r3 := ts.mustPut("c", "3")

	require.Equal(t, int64(1), r1.Header.Revision)
	require.Equal(t, int64(2), r2.Header.Revision)
	require.Equal(t, int64(3), r3.Header.Revision)
}

func Test_KVService_Put_Overwrite(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "v1")
	r2 := ts.mustPut("key", "v2")

	require.Equal(t, int64(2), r2.Header.Revision)

	// Verify the value was updated
	rangeResult := ts.mustRange(command.RangeCmd{Key: []byte("key")})
	require.Len(t, rangeResult.Entries, 1)
	require.Equal(t, "v2", string(rangeResult.Entries[0].Value))
}

func Test_KVService_Put_WithPrevEntry(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "original")
	result := ts.mustPutWithPrev("key", "updated")

	require.NotNil(t, result.PrevEntry, "PrevEntry should not be nil when requested")
	require.Equal(t, "original", string(result.PrevEntry.Value))
}

func Test_KVService_Put_WithPrevEntry_NonExistent(t *testing.T) {
	ts := newTestKVService(t)

	result := ts.mustPutWithPrev("newkey", "value")

	require.Nil(t, result.PrevEntry, "PrevEntry should be nil for a new key")
}

func Test_KVService_Put_WithPrevEntry_MultipleOverwrites(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "v1")
	ts.mustPut("key", "v2")
	result := ts.mustPutWithPrev("key", "v3")

	require.NotNil(t, result.PrevEntry)
	require.Equal(t, "v2", string(result.PrevEntry.Value), "PrevEntry should be the immediate predecessor")
}

func Test_KVService_Put_RevisionMonotonicallyIncreases(t *testing.T) {
	ts := newTestKVService(t)

	var lastRev int64
	for i := range 10 {
		result := ts.mustPut(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
		require.Greater(t, result.Header.Revision, lastRev, "revision should monotonically increase")
		lastRev = result.Header.Revision
	}
}

func Test_KVService_Put_LargeValue(t *testing.T) {
	ts := newTestKVService(t)

	largeVal := make([]byte, 64*1024) // 64KB
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}

	result, err := ts.Put(ts.ctx, command.PutCmd{
		Key:   []byte("bigkey"),
		Value: largeVal,
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	rangeResult := ts.mustRange(command.RangeCmd{Key: []byte("bigkey")})
	require.Len(t, rangeResult.Entries, 1)
	require.Equal(t, largeVal, rangeResult.Entries[0].Value)
}

func Test_KVService_Range_ExactKey(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("foo", "bar")

	result := ts.mustRange(command.RangeCmd{Key: []byte("foo")})

	require.Equal(t, 1, result.Count)
	require.Len(t, result.Entries, 1)
	require.Equal(t, "foo", string(result.Entries[0].Key))
	require.Equal(t, "bar", string(result.Entries[0].Value))
}

func Test_KVService_Range_NonExistent(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("foo", "bar")

	result := ts.mustRange(command.RangeCmd{Key: []byte("missing")})

	require.Equal(t, 0, result.Count)
	require.Empty(t, result.Entries)
}

func Test_KVService_Range_DoesNotPrefixScan(t *testing.T) {
	ts := newTestKVService(t)

	// Insert keys that share a common prefix
	ts.mustPut("f", "exact")
	ts.mustPut("f2", "nope")
	ts.mustPut("fo", "nope")
	ts.mustPut("foo", "nope")
	ts.mustPut("foobar", "nope")
	ts.mustPut("g", "nope")

	// Range with end=nil should return ONLY the exact key "f"
	result := ts.mustRange(command.RangeCmd{Key: []byte("f")})

	require.Equal(t, 1, result.Count, "point query must return exactly 1 key")
	require.Len(t, result.Entries, 1, "point query must return exactly 1 entry")
	require.Equal(t, "f", string(result.Entries[0].Key))
	require.Equal(t, "exact", string(result.Entries[0].Value))
}

func Test_KVService_Range_ExactKeyAmongSimilar(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("app", "correct")
	ts.mustPut("apple", "wrong")
	ts.mustPut("application", "wrong")
	ts.mustPut("app1", "wrong")
	ts.mustPut("ap", "wrong")

	result := ts.mustRange(command.RangeCmd{Key: []byte("app")})
	require.Len(t, result.Entries, 1)
	require.Equal(t, "app", string(result.Entries[0].Key))
	require.Equal(t, "correct", string(result.Entries[0].Value))
}

func Test_KVService_Range_WithEnd(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")
	ts.mustPut("d", "4")
	ts.mustPut("e", "5")

	// Range [b, d) should return b, c
	result := ts.mustRange(command.RangeCmd{
		Key: []byte("b"),
		End: []byte("d"),
	})

	require.Equal(t, 2, result.Count)
	require.Len(t, result.Entries, 2)
	require.Equal(t, "b", string(result.Entries[0].Key))
	require.Equal(t, "c", string(result.Entries[1].Key))
}

func Test_KVService_Range_WithEnd_Empty(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("d", "4")

	// Range [b, d) should return nothing (no keys in that range)
	result := ts.mustRange(command.RangeCmd{
		Key: []byte("b"),
		End: []byte("d"),
	})

	require.Equal(t, 0, result.Count)
	require.Empty(t, result.Entries)
}

func Test_KVService_Range_WithLimit(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")
	ts.mustPut("d", "4")
	ts.mustPut("e", "5")

	result := ts.mustRange(command.RangeCmd{
		Key:   []byte("a"),
		End:   []byte("f"),
		Limit: 3,
	})

	require.Equal(t, 5, result.Count, "Count should be the total, not limited")
	require.Len(t, result.Entries, 3, "Entries should be limited to 3")
	require.Equal(t, "a", string(result.Entries[0].Key))
	require.Equal(t, "b", string(result.Entries[1].Key))
	require.Equal(t, "c", string(result.Entries[2].Key))
}

func Test_KVService_Range_WithLimit_One(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("x", "1")
	ts.mustPut("y", "2")
	ts.mustPut("z", "3")

	result := ts.mustRange(command.RangeCmd{
		Key:   []byte("x"),
		End:   []byte("zz"),
		Limit: 1,
	})

	require.Equal(t, 3, result.Count)
	require.Len(t, result.Entries, 1)
	require.Equal(t, "x", string(result.Entries[0].Key))
}

func Test_KVService_Range_CountOnly(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")

	result := ts.mustRange(command.RangeCmd{
		Key:       []byte("a"),
		End:       []byte("d"),
		CountOnly: true,
	})

	require.Equal(t, 3, result.Count)
	require.Empty(t, result.Entries, "CountOnly should return no entries")
}

func Test_KVService_Range_EmptyStore(t *testing.T) {
	ts := newTestKVService(t)

	result := ts.mustRange(command.RangeCmd{Key: []byte("anything")})

	require.Equal(t, 0, result.Count)
	require.Empty(t, result.Entries)
}

func Test_KVService_Range_AfterOverwrite(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "v1")
	ts.mustPut("key", "v2")
	ts.mustPut("key", "v3")

	result := ts.mustRange(command.RangeCmd{Key: []byte("key")})

	require.Len(t, result.Entries, 1, "should return only the latest version")
	require.Equal(t, "v3", string(result.Entries[0].Value))
}

func Test_KVService_Range_AtRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "v1") // rev 1
	ts.mustPut("key", "v2") // rev 2
	ts.mustPut("key", "v3") // rev 3

	// Read at revision 1
	result := ts.mustRange(command.RangeCmd{
		Key:      []byte("key"),
		Revision: 1,
	})
	require.Len(t, result.Entries, 1)
	require.Equal(t, "v1", string(result.Entries[0].Value))

	// Read at revision 2
	result = ts.mustRange(command.RangeCmd{
		Key:      []byte("key"),
		Revision: 2,
	})
	require.Len(t, result.Entries, 1)
	require.Equal(t, "v2", string(result.Entries[0].Value))
}

func Test_KVService_Range_AfterDelete(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("foo", "bar")
	ts.mustDelete("foo", "", false)

	result := ts.mustRange(command.RangeCmd{Key: []byte("foo")})

	require.Equal(t, 0, result.Count, "deleted key should not appear")
	require.Empty(t, result.Entries)
}

func Test_KVService_Range_DeletedKeyAtOldRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("foo", "bar")        // rev 1
	ts.mustDelete("foo", "", false) // rev 2

	// Should still be visible at rev 1
	result := ts.mustRange(command.RangeCmd{
		Key:      []byte("foo"),
		Revision: 1,
	})
	require.Len(t, result.Entries, 1)
	require.Equal(t, "bar", string(result.Entries[0].Value))
}

func Test_KVService_Range_AllKeysOrdered(t *testing.T) {
	ts := newTestKVService(t)

	// Insert in non-alphabetical order
	ts.mustPut("cherry", "3")
	ts.mustPut("apple", "1")
	ts.mustPut("banana", "2")
	ts.mustPut("date", "4")
	ts.mustPut("zorro", "5") // excluded in [a, z)

	result := ts.mustRange(command.RangeCmd{
		Key: []byte("a"),
		End: []byte("z"),
	})

	require.Len(t, result.Entries, 4)
	// treeIndex uses a BTree, so keys should be sorted
	require.Equal(t, "apple", string(result.Entries[0].Key))
	require.Equal(t, "banana", string(result.Entries[1].Key))
	require.Equal(t, "cherry", string(result.Entries[2].Key))
	require.Equal(t, "date", string(result.Entries[3].Key))
}

func Test_KVService_Range_SingleKeyEnd(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")

	// Range [b, c) should return only b
	result := ts.mustRange(command.RangeCmd{
		Key: []byte("b"),
		End: []byte("c"),
	})

	require.Equal(t, 1, result.Count)
	require.Len(t, result.Entries, 1)
	require.Equal(t, "b", string(result.Entries[0].Key))
}

func Test_KVService_Range_EntryMetadata(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("meta", "v1") // rev 1, createRev=1, modRev=1, version=1
	ts.mustPut("meta", "v2") // rev 2, createRev=1, modRev=2, version=2

	result := ts.mustRange(command.RangeCmd{Key: []byte("meta")})

	require.Len(t, result.Entries, 1)
	entry := result.Entries[0]
	require.Equal(t, "meta", string(entry.Key))
	require.Equal(t, "v2", string(entry.Value))
	require.Equal(t, int64(1), entry.CreateRev, "CreateRev should persist from first put")
	require.Equal(t, int64(2), entry.ModRev, "ModRev should be the latest revision")
	require.Equal(t, int64(2), entry.Version, "Version should increment on each put")
}

func Test_KVService_Range_ReturnsHeaderRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1") // rev 1
	ts.mustPut("b", "2") // rev 2

	result := ts.mustRange(command.RangeCmd{Key: []byte("a")})

	require.Equal(t, int64(2), result.Header.Revision, "Header revision should be the store's current revision")
}

func Test_KVService_RangePrefix_SingleKeyEnd(t *testing.T) {
	ts := newTestKVService(t)
	ts.mustPut("f", "1")
	ts.mustPut("f2", "f1+1")
	ts.mustPut("fo", "2")
	ts.mustPut("foo", "3")
	ts.mustPut("g", "4")
	ts.mustPut("fooBar", "5")

	require.Equal(t, []byte("g"), kv.PrefixEnd([]byte("f")))

	result := ts.mustRange(command.RangeCmd{
		Key:    []byte("f"),
		End:    []byte("gets_discarded_anyways"),
		Prefix: true,
	})

	require.Equal(t, 5, result.Count)
	for _, res := range result.Entries {
		require.True(t, strings.HasPrefix(string(res.Key), "f"), "all keys should have prefix 'f'")
	}
}

func Test_KVService_RangePrefix_LongKeyEnd(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("foo", "1")
	ts.mustPut("foo2", "f1+1")
	ts.mustPut("foo/bar", "2")
	ts.mustPut("foobar", "3")
	ts.mustPut("fo", "4")
	ts.mustPut("fop", "5")

	require.Equal(t, []byte("fop"), kv.PrefixEnd([]byte("foo")))

	result := ts.mustRange(command.RangeCmd{
		Key:    []byte("foo"),
		Prefix: true,
	})

	require.Equal(t, 4, result.Count)
	for _, res := range result.Entries {
		require.True(t, strings.HasPrefix(string(res.Key), "foo"), "all keys should have prefix 'foo'")
	}
}

func Test_KVService_RangePrefix_NoMatchingKeys(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")

	result := ts.mustRange(command.RangeCmd{
		Key:    []byte("d"),
		Prefix: true,
	})

	require.Equal(t, 0, result.Count)
}

func Test_KVService_RangePrefix_All0xFF_Prefix(t *testing.T) {
	ts := newTestKVService(t)

	n := 10
	for i := range n - 1 {
		s := fmt.Sprintf("%d", i)
		ts.mustPut(s, s)
	}
	ts.mustPut("\xff\xff", "bar")

	result := ts.mustRange(command.RangeCmd{
		Key:    []byte{}, // empty prefix -> PrefixEnd returns nil
		Prefix: true,
	})

	require.Equal(t, n, result.Count, "empty prefix should match any key")
}

//TODO: more prefix edge case tests

func Test_KVService_Delete_Single(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("foo", "bar")
	result := ts.mustDelete("foo", "", false)

	require.Equal(t, int64(1), result.NumDeleted)
}

func Test_KVService_Delete_NonExistent(t *testing.T) {
	ts := newTestKVService(t)

	result := ts.mustDelete("nope", "", false)

	require.Equal(t, int64(0), result.NumDeleted)
}

func Test_KVService_Delete_Range(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")
	ts.mustPut("d", "4")

	// Delete [b, d) => deletes b, c
	result := ts.mustDelete("b", "d", false)

	require.Equal(t, int64(2), result.NumDeleted)

	// Verify remaining keys
	remaining := ts.mustRange(command.RangeCmd{Key: []byte("a"), End: []byte("z")})
	require.Len(t, remaining.Entries, 2)
	require.Equal(t, "a", string(remaining.Entries[0].Key))
	require.Equal(t, "d", string(remaining.Entries[1].Key))
}

func Test_KVService_Delete_WithPrevEntries(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("x", "xval")
	ts.mustPut("y", "yval")

	result := ts.mustDelete("x", "z", true)

	require.Equal(t, int64(2), result.NumDeleted)
	require.Len(t, result.PrevEntries, 2)
	require.Equal(t, "xval", string(result.PrevEntries[0].Value))
	require.Equal(t, "yval", string(result.PrevEntries[1].Value))
}

func Test_KVService_Delete_WithPrevEntries_NonExistent(t *testing.T) {
	ts := newTestKVService(t)

	result := ts.mustDelete("ghost", "", true)

	require.Equal(t, int64(0), result.NumDeleted)
	require.Empty(t, result.PrevEntries)
}

func Test_KVService_Delete_ThenRange(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "value")
	ts.mustDelete("key", "", false)

	result := ts.mustRange(command.RangeCmd{Key: []byte("key")})
	require.Equal(t, 0, result.Count, "deleted key should not be returned by Range")
}

func Test_KVService_Delete_DoesNotAffectOtherKeys(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("keep1", "v1")
	ts.mustPut("delete_me", "v2")
	ts.mustPut("keep2", "v3")

	ts.mustDelete("delete_me", "", false)

	r1 := ts.mustRange(command.RangeCmd{Key: []byte("keep1")})
	require.Len(t, r1.Entries, 1)
	require.Equal(t, "v1", string(r1.Entries[0].Value))

	r2 := ts.mustRange(command.RangeCmd{Key: []byte("keep2")})
	require.Len(t, r2.Entries, 1)
	require.Equal(t, "v3", string(r2.Entries[0].Value))
}

func Test_KVService_Delete_EmptyRange(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("z", "26")

	// Delete range [m, n) — no keys exist there
	result := ts.mustDelete("m", "n", false)

	require.Equal(t, int64(0), result.NumDeleted)
}

func Test_KVService_PutDeletePut(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "v1")         // rev 1
	ts.mustDelete("key", "", false) // rev 2
	ts.mustPut("key", "v2")         // rev 3

	result := ts.mustRange(command.RangeCmd{Key: []byte("key")})
	require.Len(t, result.Entries, 1)
	require.Equal(t, "v2", string(result.Entries[0].Value))

	entry := result.Entries[0]
	require.Equal(t, int64(3), entry.CreateRev, "re-created key should have new CreateRev")
	require.Equal(t, int64(3), entry.ModRev)
	require.Equal(t, int64(1), entry.Version, "re-created key should reset version to 1")
}

func Test_KVService_VersionTracking(t *testing.T) {
	ts := newTestKVService(t)

	for i := 1; i <= 5; i++ {
		ts.mustPut("counter", fmt.Sprintf("v%d", i))
	}

	result := ts.mustRange(command.RangeCmd{Key: []byte("counter")})
	require.Len(t, result.Entries, 1)

	entry := result.Entries[0]
	require.Equal(t, int64(5), entry.Version, "version should be 5 after 5 puts")
	require.Equal(t, int64(1), entry.CreateRev, "createRev should be 1")
	require.Equal(t, int64(5), entry.ModRev, "modRev should be 5")
}

func Test_KVService_ManyKeysRangeAll(t *testing.T) {
	ts := newTestKVService(t)

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	for i, k := range keys {
		ts.mustPut(k, fmt.Sprintf("val_%d", i))
	}

	result := ts.mustRange(command.RangeCmd{
		Key: []byte("a"),
		//End: []byte("zz"), // TODO: z stops "zeta" from being included, "zz" does include "zeta" (since z > e)
		End: []byte("{"), // '{' is the next ASCII char after 'z', so it will include all keys starting with alphanumerics
	})

	require.Equal(t, len(keys), result.Count)
	require.Len(t, result.Entries, len(keys))
}

func Test_KVService_RangeRevisionConsistency(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1") // rev 1
	ts.mustPut("b", "2") // rev 2
	ts.mustPut("c", "3") // rev 3

	// At revision 2, only a and b should exist
	result := ts.mustRange(command.RangeCmd{
		Key:      []byte("a"),
		End:      []byte("z"),
		Revision: 2,
	})
	require.Equal(t, 2, result.Count)
	require.Len(t, result.Entries, 2)
	require.Equal(t, "a", string(result.Entries[0].Key))
	require.Equal(t, "b", string(result.Entries[1].Key))

	// At revision 1, only a should exist
	result = ts.mustRange(command.RangeCmd{
		Key:      []byte("a"),
		End:      []byte("z"),
		Revision: 1,
	})
	require.Equal(t, 1, result.Count)
	require.Len(t, result.Entries, 1)
	require.Equal(t, "a", string(result.Entries[0].Key))
}

func Test_KVService_DeleteThenRangeAtOldRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")          // rev 1
	ts.mustPut("b", "2")          // rev 2
	ts.mustPut("c", "3")          // rev 3
	ts.mustDelete("b", "", false) // rev 4

	// At current revision, b should be gone
	result := ts.mustRange(command.RangeCmd{
		Key: []byte("a"),
		End: []byte("d"),
	})
	require.Equal(t, 2, result.Count)

	// At revision 3, b should still exist
	result = ts.mustRange(command.RangeCmd{
		Key:      []byte("a"),
		End:      []byte("d"),
		Revision: 3,
	})
	require.Equal(t, 3, result.Count)
	require.Equal(t, "b", string(result.Entries[1].Key))
}

func Test_KVService_Range_PrefixScan_MultiplePatterns(t *testing.T) {
	ts := newTestKVService(t)

	// Set up keys with tricky prefixes
	keysAndVals := map[string]string{
		"a":       "single",
		"aa":      "double",
		"aaa":     "triple",
		"ab":      "different",
		"b":       "other",
		"/path":   "slash1",
		"/path/a": "slash2",
	}

	// Put all keys (skip empty if unsupported)
	for k, v := range keysAndVals {
		ts.mustPut(k, v)
	}

	// Point query for "a" should return ONLY "a"
	result := ts.mustRange(command.RangeCmd{Key: []byte("a")})
	require.Len(t, result.Entries, 1, "point query for 'a' should return exactly 1")
	require.Equal(t, "a", string(result.Entries[0].Key))
	require.Equal(t, "single", string(result.Entries[0].Value))

	// Point query for "aa" should return ONLY "aa"
	result = ts.mustRange(command.RangeCmd{Key: []byte("aa")})
	require.Len(t, result.Entries, 1, "point query for 'aa' should return exactly 1")
	require.Equal(t, "aa", string(result.Entries[0].Key))

	// Point query for "/path" should return ONLY "/path"
	result = ts.mustRange(command.RangeCmd{Key: []byte("/path")})
	require.Len(t, result.Entries, 1, "point query for '/path' should return exactly 1")
	require.Equal(t, "/path", string(result.Entries[0].Key))
}

func Test_KVService_Range_EndBoundaryIsExclusive(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("a", "1")
	ts.mustPut("b", "2")
	ts.mustPut("c", "3")

	// Range [a, c) should NOT include c
	result := ts.mustRange(command.RangeCmd{
		Key: []byte("a"),
		End: []byte("c"),
	})

	require.Equal(t, 2, result.Count)
	for _, e := range result.Entries {
		require.NotEqual(t, "c", string(e.Key), "end boundary should be exclusive")
	}
}

func Test_KVService_Range_LimitZeroMeansNoLimit(t *testing.T) {
	ts := newTestKVService(t)

	for i := 0; i < 20; i++ {
		ts.mustPut(fmt.Sprintf("key%02d", i), fmt.Sprintf("val%d", i))
	}

	result := ts.mustRange(command.RangeCmd{
		Key:   []byte("key00"),
		End:   []byte("key99"),
		Limit: 0,
	})

	require.Equal(t, 20, result.Count)
	require.Len(t, result.Entries, 20)
}

func Test_KVService_Range_FutureRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "value") // rev 1

	_, err := ts.Range(ts.ctx, command.RangeCmd{
		Key:      []byte("key"),
		Revision: 9999,
	})
	require.Error(t, err, "querying a future revision should fail")
}

// ==================== Header field tests ====================

func Test_KVService_HeaderFields(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "value")

	result := ts.mustRange(command.RangeCmd{Key: []byte("key")})

	require.NotEmpty(t, result.Header.NodeID, "NodeID should be set")
	require.Greater(t, result.Header.Revision, int64(0), "Revision should be positive")
}

func Test_KVService_Put_HeaderRevisionMatchesRange(t *testing.T) {
	ts := newTestKVService(t)

	putResult := ts.mustPut("key", "value")
	rangeResult := ts.mustRange(command.RangeCmd{Key: []byte("key")})

	require.Equal(t, putResult.Header.Revision, rangeResult.Header.Revision,
		"put result revision should match range header revision")
}

func Test_KVService_Delete_HeaderRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "value")                   // rev 1
	delResult := ts.mustDelete("key", "", false) // rev 2

	require.Equal(t, int64(2), delResult.Header.Revision)
}

// tests for IgnoreValue | IgnoreLease

func Test_KVService_Put_IgnoreValue_UpdatesLeaseOnly(t *testing.T) {
	ts := newTestKVService(t)

	l, err := ts.lm.Grant(0, 60)
	require.NoError(t, err)

	ts.mustPut("foo", "bar")

	// now update just the lease
	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("foo"),
		LeaseID:     l.ID,
		IgnoreValue: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// value should be unchanged
	rangeResult := ts.mustRange(command.RangeCmd{Key: []byte("foo")})
	require.Len(t, rangeResult.Entries, 1)
	entry := rangeResult.Entries[0]
	require.Equal(t, "bar", string(entry.Value), "value should be preserved")
	require.Equal(t, l.ID, entry.LeaseID, "lease should be updated")
}

func Test_KVService_Put_IgnoreValue_NonExistentKey_ReturnsError(t *testing.T) {
	ts := newTestKVService(t)

	_, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("nonexistent"),
		IgnoreValue: true,
	})
	require.Error(t, err, "expected error on nonexistent key when IgnoreLease is set")
}

func Test_KVService_Put_IgnoreValue_PreservesValueAcrossMultiplePuts(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "original")

	// multiple IgnoreValue puts should keep bumping revision but preserving value
	for range 3 {
		result, err := ts.Put(ts.ctx, api.KvPutRequest{
			Key:         []byte("key"),
			IgnoreValue: true,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
	}

	rangeResult := ts.mustRange(command.RangeCmd{Key: []byte("key")})
	require.Len(t, rangeResult.Entries, 1)
	entry := rangeResult.Entries[0]
	require.Equal(t, "original", string(entry.Value), "value should still be original")
	require.Equal(t, int64(4), entry.Version, "version should be 4 after 3 extra puts")
	require.Equal(t, int64(1), entry.CreateRev, "createRev should not change")
}

func Test_KVService_Put_IgnoreValue_BumpsRevision(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "value") // rev 1

	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("key"),
		IgnoreValue: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Header.Revision, "should bump revision even with IgnoreValue")
}

func Test_KVService_Put_IgnoreValue_WithPrevEntry(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "original")

	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("key"),
		IgnoreValue: true,
		PrevEntry:   true,
	})
	require.NoError(t, err)
	require.NotNil(t, result.PrevEntry, "PrevEntry should be populated when requested")
	require.Equal(t, "original", string(result.PrevEntry.Value))
}

// ==================== IgnoreLease tests ====================

func Test_KVService_Put_IgnoreLease_PreservesExistingLease(t *testing.T) {
	ts := newTestKVService(t)

	l, err := ts.lm.Grant(0, 60)
	require.NoError(t, err)

	_, err = ts.Put(ts.ctx, api.KvPutRequest{
		Key:     []byte("foo"),
		Value:   []byte("bar"),
		LeaseID: l.ID,
	})
	require.NoError(t, err)

	// update value but keep lease
	_, err = ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("foo"),
		Value:       []byte("newbar"),
		IgnoreLease: true,
	})
	require.NoError(t, err)

	rangeResult := ts.mustRange(api.KvRangeRequest{Key: []byte("foo")})
	require.Len(t, rangeResult.Entries, 1)
	entry := rangeResult.Entries[0]
	require.Equal(t, "newbar", string(entry.Value), "value should be updated")
	require.Equal(t, l.ID, entry.LeaseID, "lease should be preserved")
}

func Test_KVService_Put_IgnoreLease_NonExistentKey_ReturnsError(t *testing.T) {
	ts := newTestKVService(t)

	_, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("nonexistent"),
		Value:       []byte("value"),
		IgnoreLease: true,
	})
	require.Error(t, err, "expected error on nonexistent key when IgnoreLease is set")
}

func Test_KVService_Put_IgnoreLease_KeyWithNoLease_PreservesNoLease(t *testing.T) {
	ts := newTestKVService(t)

	ts.mustPut("key", "value")

	// IgnoreLease on a key with no lease should just keep leaseID=0
	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("key"),
		Value:       []byte("newvalue"),
		IgnoreLease: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	rangeResult := ts.mustRange(command.RangeCmd{Key: []byte("key")})
	entry := rangeResult.Entries[0]
	require.Equal(t, "newvalue", string(entry.Value))
	require.Equal(t, int64(0), entry.LeaseID, "leaseID should still be 0")
}

func Test_KVService_Put_IgnoreLease_DoesNotDetachExistingLease(t *testing.T) {
	ts := newTestKVService(t)

	l, err := ts.lm.Grant(0, 60)
	require.NoError(t, err)

	_, err = ts.Put(ts.ctx, command.PutCmd{
		Key:     []byte("foo"),
		Value:   []byte("v1"),
		LeaseID: l.ID,
	})
	require.NoError(t, err)

	// lease should stay attached
	_, err = ts.Put(ts.ctx, command.PutCmd{
		Key:         []byte("foo"),
		Value:       []byte("v2"),
		IgnoreLease: true,
	})
	require.NoError(t, err)

	lease := ts.lm.Lookup(l.ID)
	require.NotNil(t, lease)
	require.Equal(t, 1, len(lease.KeySet()), "expected key should still be attached to lease")
}

//  IgnoreValue & IgnoreLease combined

// touch: bump revision/version without changing anything
func Test_KVService_Put_IgnoreValueAndLease_ActsAsTouchOperation(t *testing.T) {
	ts := newTestKVService(t)

	l, err := ts.lm.Grant(0, 60)
	require.NoError(t, err)

	_, err = ts.Put(ts.ctx, api.KvPutRequest{
		Key:     []byte("foo"),
		Value:   []byte("original"),
		LeaseID: l.ID,
	})
	require.NoError(t, err)

	// touch
	result, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("foo"),
		IgnoreValue: true,
		IgnoreLease: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Header.Revision)

	rangeResult := ts.mustRange(api.KvRangeRequest{Key: []byte("foo")})
	entry := rangeResult.Entries[0]
	require.Equal(t, "original", string(entry.Value), "value unchanged")
	require.Equal(t, l.ID, entry.LeaseID, "lease unchanged")
	require.Equal(t, int64(2), entry.Version, "version bumped")
	require.Equal(t, int64(1), entry.CreateRev, "createRev unchanged")
	require.Equal(t, int64(2), entry.ModRev, "modRev updated")
}

func Test_KVService_Put_IgnoreValueAndLease_NonExistentKey_ReturnsError(t *testing.T) {
	ts := newTestKVService(t)

	_, err := ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("ghost"),
		IgnoreValue: true,
		IgnoreLease: true,
	})
	require.Error(t, err, "expected error on nonexistent key when IgnoreLease or IgnoreValue is set")
}

func Test_KVService_Put_IgnoreValueAndLease_PreservesAllFieldsExceptRevAndVersion(t *testing.T) {
	ts := newTestKVService(t)

	l, err := ts.lm.Grant(0, 60)
	require.NoError(t, err)

	ts.mustPut("foo", "value1") // rev 1

	_, err = ts.Put(ts.ctx, api.KvPutRequest{
		Key:     []byte("foo"),
		Value:   []byte("value2"),
		LeaseID: l.ID,
	})
	require.NoError(t, err) // rev 2

	_, err = ts.Put(ts.ctx, api.KvPutRequest{
		Key:         []byte("foo"),
		IgnoreValue: true,
		IgnoreLease: true,
	})
	require.NoError(t, err) // rev 3

	rangeResult := ts.mustRange(api.KvRangeRequest{Key: []byte("foo")})
	entry := rangeResult.Entries[0]
	require.Equal(t, "value2", string(entry.Value))
	require.Equal(t, l.ID, entry.LeaseID)
	require.Equal(t, int64(1), entry.CreateRev)
	require.Equal(t, int64(3), entry.ModRev)
	require.Equal(t, int64(3), entry.Version)
}
