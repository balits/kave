package lease

import (
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

func lmWithClock(t *testing.T, c util.Clock) *LeaseManager {
	t.Helper()
	lm := newTestLm(t)
	lm.clock = c // very ugly... but works for now
	return lm
}

func newTestLm(t *testing.T) *LeaseManager {
	logger := slog.Default()
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, storage.Options{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	store := mvcc.NewKvStore(reg, logger, backend)
	t.Cleanup(func() { backend.Close() })
	return NewManager(reg, logger, store, backend)
}

func Test_LeaseManager_Grant(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	tests := []struct {
		id  int64
		ttl int64
	}{
		{
			id:  1,
			ttl: 5,
		},
		{
			id:  0,
			ttl: 5,
		},
	}

	for _, test := range tests {
		l, err := lm.Grant(test.id, test.ttl)
		require.NoError(t, err, "lease grant")
		require.Equal(t, test.ttl, l.TTL, "lease grant")
		require.Equal(t, test.ttl, l.remainingTTL, "lease grant")

		l2 := lm.Lookup(l.ID)
		require.Equal(t, l.ID, l2.ID, "after Lookup inmem")
		require.Equal(t, l.TTL, l2.TTL, "after Lookup inmem")
		t.Logf("new lease with ID: requested=%d, got=%d", test.id, l.ID)

		l3 := unsafeGetFromBackend(lm, l.ID)
		require.Equal(t, l.ID, l3.ID, "after unsafeGetFromBackend")
		require.Equal(t, l.TTL, l3.TTL, "after unsafeGetFromBackend")
	}

	_, err := lm.Grant(tests[0].id, tests[0].ttl)
	require.Error(t, err, "lease grant: wanted error on id conflict")

	_, err = lm.Grant(0, -5)
	require.Error(t, err, "lease grant: wanted error on negative ttl")

	l, err := lm.Grant(0, maxTTL+100)
	require.NoError(t, err, "lease grant")
	require.Equal(t, maxTTL, l.TTL, "lease grant: wanted l.TTL to be maxTTL")
}

func Test_LeaseManager_Revoke(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	l, err := lm.Grant(1, 5)
	require.NoError(t, err, "lease grant")
	l2 := lm.Lookup(l.ID)
	require.Equal(t, l.ID, l2.ID)
	require.Equal(t, l.TTL, l2.TTL)

	found, revoked, err := lm.Revoke(0)
	require.NoError(t, err, "lease revoke: unexpected error")
	require.False(t, found, "lease revoke: expected false values after revoking non existent lease")
	require.False(t, revoked, "lease revoke: expected false values after revoking non existent lease")

	found, revoked, err = lm.Revoke(l.ID)
	require.NoError(t, err, "lease revoke: unexpected error")
	require.True(t, found, "lease revoke: expected true values after revoking existing lease")
	require.True(t, revoked, "lease revoke: expected true values after revoking existing lease")
	require.Nil(t, unsafeGetFromBackend(lm, l.ID), "lease revoke: expected nil from backend after revoking existing lease")

	l3 := lm.Lookup(l.ID)
	require.Nil(t, l3, "lease revoke: wanted nil on looking up revoked lease")
}

func Test_LeaseManager_KeepAlive(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	l, err := lm.Grant(0, 5)
	require.NoError(t, err, "lease grant")
	oldExpiry := l.expiry
	time.After(time.Second * 2)

	rem, err := lm.KeepAlive(l.ID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, int64(time.Until(l.expiry).Seconds()), rem, "lease keep alive: ttl shouldve updated")
	require.Greater(t, l.expiry, oldExpiry, "lease keep alive: expiry shouldve updated")
	require.GreaterOrEqual(t, rem, l.remainingTTL, "lease keep alive: remTTL shouldve updated")

	_, err = lm.KeepAlive(-2)
	require.Error(t, err, "lease keep alive: wanted error on lease not found")
}

func Test_LeaseManager_AttachKey(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)
	l, err := lm.Grant(0, 5)
	require.NoError(t, err, "lease grant")

	lm.AttachKey(l.ID, []byte("foo"))
	lm.AttachKey(l.ID, []byte("bar"))

	require.Equal(t, 2, len(l.keySet), "attach key: wanted Lease's keys map size to increase")
}

func Test_LeaseManager_DetachKey(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)
	l, err := lm.Grant(0, 5)
	require.NoError(t, err, "lease grant")

	lm.AttachKey(l.ID, []byte("foo"))
	require.Equal(t, 1, len(l.keySet), "attach key: wanted Lease's keys map size to increase")

	lm.DetachKey(l.ID, []byte("foo"))
	require.Equal(t, 0, len(l.keySet), "attach key: wanted Lease's keys map size to decrease")
}

func Test_LeaseManager_Checkpoint(t *testing.T) {
	t.Parallel()
	c := util.NewFakeClock(time.Now()).(*util.FakeClock)
	lm := lmWithClock(t, c)

	lm.Grant(1, 1)
	lm.Grant(2, 5)
	lm.Grant(3, 6)
	lm.Grant(4, 7)

	cs := lm.Checkpoint()
	require.Equal(t, 4, len(cs), "checkpoint")
	for _, c := range cs {
		t.Log(c.LeaseID, c.RemainingTTL)
		require.Equal(t, c.RemainingTTL, lm.Lookup(c.LeaseID).remainingTTL, "wanted remaining ttl to be update")
	}

	advancedSec := 5
	c.AdvanceSeconds(time.Duration(advancedSec))

	cs = lm.Checkpoint()
	require.Equal(t, 2, len(cs), "checkpoint")
	for _, c := range cs {
		t.Log(c.LeaseID, c.RemainingTTL)
		require.Equal(t, c.RemainingTTL, lm.leaseMap[c.LeaseID].remainingTTL-int64(advancedSec), "wanted remaining ttl to be update")
	}

	lm.ApplyCheckpoint(command.CmdLeaseCheckpoint{Checkpoints: cs})
	for _, c := range cs {
		t.Log(c.LeaseID, c.RemainingTTL)
		require.Equal(t, c.RemainingTTL, lm.Lookup(c.LeaseID).remainingTTL, "wanted remaining ttl to be update")
	}

	c.AdvanceSeconds(5)

	cs = lm.Checkpoint()
	require.Equal(t, 0, len(cs), "checkpoint")
}

func Test_LeaseManager_DrainExpiredLeases(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, c)
	lm.Grant(1, 5)
	lm.Grant(2, 10)
	lm.Grant(3, 15)
	lm.Grant(4, 20)

	expired := lm.DrainExpiredLeases()
	require.Equal(t, 0, len(expired), "expired leases: no leases should be expired")

	for range 4 {
		c.AdvanceSeconds(6)
		expired = lm.DrainExpiredLeases()
		require.Equal(t, 1, len(expired))
	}
}

func Test_LeaseManager_ApplyExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, c)
	lm.Grant(1, 5)
	lm.Grant(2, 10)
	lm.Grant(3, 15)
	lm.Grant(4, 20)

	t.Log("before advance", c.Now())
	c.AdvanceSeconds(11)
	t.Log("after  advance", c.Now())
	expired := lm.DrainExpiredLeases()
	require.Equal(t, 2, len(expired))

	ids := make([]int64, 0)
	for _, l := range expired {
		ids = append(ids, l.ID)
	}

	res, err := lm.ApplyExpired(command.CmdLeaseExpire{ExpiredIDs: ids})
	require.NoError(t, err)
	require.Equal(t, len(ids), res.RemovedLeaseCount)

	for _, id := range ids {
		require.Nil(t, lm.Lookup(id), "expected lease to be deleted after ApplyExpired")
	}
}

func Test_Restore_BasicRoundTrip_NoKeys(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	l1, _ := lm.Grant(1, 30)
	l2, _ := lm.Grant(2, 60)
	l3, _ := lm.Grant(3, 90)

	simulateRestart(t, lm)

	for _, want := range []*Lease{l1, l2, l3} {
		got := lm.Lookup(want.ID)
		require.NotNil(t, got, "lease %d should exist after restore", want.ID)
		require.Equal(t, want.TTL, got.TTL, "TTL should be preserved for lease %d", want.ID)
		require.Greater(t, got.remainingTTL, int64(0), "remainingTTL should be positive for lease %d", want.ID)
	}
}

func Test_LeaseManager_Restore_BasicRoundTrip_WithKeys(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, c)
	lm.Grant(1, 5)           // dead
	lm.Grant(2, 10)          // dead
	l3, _ := lm.Grant(3, 15) // kept alive
	lm.Grant(4, 20)

	{
		w := lm.store.NewWriter()
		w.Put([]byte("foo1"), []byte("bar"), l3.ID)
		lm.AttachKey(l3.ID, []byte("foo1"))
		w.Put([]byte("foo2"), []byte("bar"), l3.ID)
		lm.AttachKey(l3.ID, []byte("foo2"))
		w.End()
	}

	require.Equal(t, 2, len(l3.keySet))
	var (
		advanceTotal   = 16
		firstAdvance   = 11
		secondAdvacnce = advanceTotal - firstAdvance
	)
	c.AdvanceSeconds(time.Duration(firstAdvance))
	remTTL, err := lm.KeepAlive(l3.ID)
	require.NoError(t, err, "keep-alive")
	require.Equal(t, l3.TTL, remTTL, "wanted keep-alive to update remTTL to be lease.TTL")
	c.AdvanceSeconds(time.Duration(secondAdvacnce))

	cs := lm.Checkpoint()
	require.Equal(t, 2, len(cs), "checkpoint")
	for _, c := range cs {
		t.Log("id", c.LeaseID, "remTTL", c.RemainingTTL)
		switch c.LeaseID {
		case 3:
			require.Equal(t, c.RemainingTTL, lm.leaseMap[c.LeaseID].remainingTTL-int64(secondAdvacnce), "wanted checkpont to update remTTL to be remTTL-secondAdvance (since we kept lease3 alive after first advance)")
		case 4:
			require.Equal(t, c.RemainingTTL, lm.leaseMap[c.LeaseID].remainingTTL-int64(advanceTotal), "wanted checkpont to update remTTL to be remTTL-advanceTotal (since lease4 was never kept alive, but its ttl was > advanceTotal)")
		default:
			t.Fatal("unexpected ID in checkpoint:", c.LeaseID)
		}
	}
	lm.ApplyCheckpoint(command.CmdLeaseCheckpoint{Checkpoints: cs})
	for _, c := range cs {
		t.Log(c.LeaseID, c.RemainingTTL)
		require.Equal(t, c.RemainingTTL, lm.Lookup(c.LeaseID).remainingTTL, "wanted remaining ttl to be update")
	}
	ids := make([]int64, 0, 2)
	for _, l := range lm.DrainExpiredLeases() {
		ids = append(ids, l.ID)
	}
	res, err := lm.ApplyExpired(command.CmdLeaseExpire{
		ExpiredIDs: ids,
	})
	require.NoError(t, err, "apply expired")
	require.NotNil(t, lm.Lookup(3), "lease3 shouldve survived restore (was kept alive)")
	require.NotNil(t, lm.Lookup(4), "lease4 shouldve survived restore (ttl was larger than clock.Advance())")

	simulateRestart(t, lm)

	require.Nil(t, lm.Lookup(1), "lease1 shouldve been dropped while restoring")
	require.Nil(t, lm.Lookup(2), "lease2 shouldve been dropped while restoring")
	require.Equal(t, len(ids), res.RemovedLeaseCount, "two leases shouldve been expired")
	require.NotNil(t, lm.Lookup(3), "lease3 shouldve survived restore (was kept alive)")
	require.NotNil(t, lm.Lookup(4), "lease4 shouldve survived restore (ttl was larger than clock.Advance())")

	t.Log(lm.Lookup(3).keySet)
	require.Equal(t, 2, len(lm.Lookup(3).keySet), "lease3 keyset should have 2 keys")
}

func Test_LeaseManager_Restore_ExpiredLeaseIsNotRestored(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	l, err := lm.Grant(99, 10)
	require.NoError(t, err)

	l.remainingTTL = 0
	require.NoError(t, lm.unsafePersistToBackend(l))

	simulateRestart(t, lm)

	require.Nil(t, lm.Lookup(99))
}

func Test_Restore_HeapOrderIsPreserved(t *testing.T) {
	t.Parallel()
	now := time.Now()
	fc := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, fc)

	lm.Grant(1, 5)
	lm.Grant(2, 10)
	lm.Grant(3, 15)
	lm.Grant(4, 20)

	simulateRestart(t, lm)

	expectedOrder := []int64{1, 2, 3, 4}
	for _, expectedID := range expectedOrder {
		fc.AdvanceSeconds(6)
		expired := lm.DrainExpiredLeases()
		require.Len(t, expired, 1, "expected exactly one expired lease per 6s advance")
		require.Equal(t, expectedID, expired[0].ID, "expected lease %d to expire, got %d", expectedID, expired[0].ID)
	}
}

func Test_Restore_OrphanedKeyIsSkipped(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	nonExistentLeaseID := int64(999)
	w := lm.store.NewWriter()
	w.Put([]byte("orphan1"), []byte("bar"), nonExistentLeaseID)
	w.Put([]byte("orphan2"), []byte("bar"), nonExistentLeaseID)
	w.End()

	require.NotPanics(t, func() {
		simulateRestart(t, lm)
	}, "Restore should not panic on orphaned key")

	require.Nil(t, lm.Lookup(nonExistentLeaseID),
		"non-existent lease should not appear after restore")
}

func Test_Restore_CorruptLeaseRecordIsSkipped(t *testing.T) {
	t.Parallel()
	lm := newTestLm(t)

	lm.Grant(10, 30)
	lm.Grant(20, 60)

	{
		key := EncodeLeaseBucketKey(LeaseBucketKey(15))
		wtx := lm.backend.WriteTx()
		wtx.Lock()
		require.NoError(t, wtx.UnsafePut(schema.BucketLease, key, []byte("garbage")))
		_, err := wtx.Commit()
		require.NoError(t, err)
		wtx.Unlock()
	}

	require.NoError(t, lm.Restore(), "restore should not fail on a single corrupt record")
	require.NotNil(t, lm.Lookup(10), "lease10 shouldve survived")
	require.NotNil(t, lm.Lookup(20), "lease20 should survive")
	require.Nil(t, lm.Lookup(15), "corrupt record should return a valid lease")
}

func simulateRestart(t *testing.T, lm *LeaseManager) {
	t.Helper()
	clear(lm.leaseMap)
	clear(lm.heapMap)
	newHeap := NewLeaseHeap()
	lm.heap = &newHeap
	require.NoError(t, lm.Restore(), "restore")
}

func unsafeGetFromBackend(lm *LeaseManager, id int64) *Lease {
	bucketKey := EncodeLeaseBucketKey(LeaseBucketKey(id))

	r := lm.backend.ReadTx()
	r.RLock()
	defer r.RUnlock()
	bs, err := r.UnsafeGet(schema.BucketLease, bucketKey)
	if err != nil {
		return nil
	}

	l, err := DecodeLease(bs)
	if err != nil {
		return nil
	}

	return l
}
