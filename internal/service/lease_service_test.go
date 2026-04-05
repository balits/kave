package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
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

type testLeaseService struct {
	LeaseService
	t   *testing.T
	ctx context.Context
}

func newTestLeaseService(t *testing.T) *testLeaseService {
	t.Helper()
	logger := slog.Default()
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	kvstore := mvcc.NewKvStore(reg, logger, backend)
	t.Cleanup(func() { backend.Close() })
	lm := lease.NewManager(reg, logger, kvstore, backend)
	fsm := fsm.New(logger, kvstore, lm, nil, "test")

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

	svc := NewLeaseService(logger, propose)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	return &testLeaseService{
		LeaseService: svc,
		t:            t,
		ctx:          ctx,
	}
}

func (ls *testLeaseService) mustGrant(id, ttl int64) *api.LeaseGrantResponse {
	ls.t.Helper()
	result, err := ls.Grant(ls.ctx, api.LeaseGrantRequest{
		LeaseID: id,
		TTL:     ttl,
	})

	require.NoError(ls.t, err, "Grant(%q, %q) failed", id, ttl)
	require.NotNil(ls.t, result)
	return result
}

func (ls *testLeaseService) mustRevoke(id int64) *api.LeaseRevokeResponse {
	ls.t.Helper()
	result, err := ls.Revoke(ls.ctx, api.LeaseRevokeRequest{
		LeaseID: id,
	})

	require.NoError(ls.t, err, "Revoke(%q) failed", id)
	require.NotNil(ls.t, result)
	return result
}

func (ls *testLeaseService) mustKeepAlive(id int64) *api.LeaseKeepAliveResponse {
	ls.t.Helper()
	result, err := ls.KeepAlive(ls.ctx, api.LeaseKeepAliveRequest{
		LeaseID: id,
	})

	require.NoError(ls.t, err, "KeepAlive(%q) failed", id)
	require.NotNil(ls.t, result)
	return result
}

func (ls *testLeaseService) mustLookup(id int64) *api.LeaseLookupResponse {
	ls.t.Helper()
	result, err := ls.Lookup(ls.ctx, api.LeaseLookupRequest{
		LeaseID: id,
	})

	require.NoError(ls.t, err, "Lookup(%q) failed", id)
	require.NotNil(ls.t, result)
	return result
}

func Test_LeaseService_Grant(t *testing.T) {
	t.Parallel()
	ls := newTestLeaseService(t)

	res := ls.mustGrant(-1, 10)
	require.Equal(t, int64(-1), res.LeaseID)
	require.Equal(t, int64(10), res.TTL)

	for i := range int64(5) {
		res := ls.mustGrant(i+1, 10)
		require.Equal(t, int64(i+1), res.LeaseID)
		require.Equal(t, int64(10), res.TTL)
	}

	_, err := ls.Grant(ls.ctx, api.LeaseGrantRequest{
		LeaseID: -1,
		TTL:     10,
	})
	require.Error(ls.t, err, "expected ID collision")

	_, err = ls.Grant(ls.ctx, api.LeaseGrantRequest{LeaseID: 0, TTL: 0})
	require.Error(ls.t, err)
}

func Test_LeaseService_Revoke(t *testing.T) {
	t.Parallel()
	ls := newTestLeaseService(t)

	resg := ls.mustGrant(0, 10)
	require.Equal(t, int64(10), resg.TTL)

	resr := ls.mustRevoke(resg.LeaseID)
	require.True(t, resr.Found)
	require.True(t, resr.Revoked)

	resr = ls.mustRevoke(resg.LeaseID)
	require.False(t, resr.Found)
	require.False(t, resr.Revoked)

	resg = ls.mustGrant(1, 1)
	require.Equal(t, int64(1), resg.LeaseID)
	require.Equal(t, int64(1), resg.TTL)
}

func Test_LeaseService_KeepAlive(t *testing.T) {
	t.Parallel()
	ls := newTestLeaseService(t)

	resGranted := ls.mustGrant(-1, 10)
	require.Equal(t, int64(-1), resGranted.LeaseID)
	require.Equal(t, int64(10), resGranted.TTL)

	resKeepAlive := ls.mustKeepAlive(resGranted.LeaseID)
	require.Equal(t, resGranted.LeaseID, resKeepAlive.LeaseID)
	require.InDelta(t, resKeepAlive.TTL, resGranted.TTL, 1)

	_, err := ls.KeepAlive(ls.ctx, api.LeaseKeepAliveRequest{LeaseID: 0})
	require.Error(ls.t, err)
}

func Test_LeaseService_Lookup(t *testing.T) {
	t.Parallel()
	ls := newTestLeaseService(t)

	resGranted := ls.mustGrant(-1, 10)
	require.Equal(t, int64(-1), resGranted.LeaseID)
	require.Equal(t, int64(10), resGranted.TTL)

	resLookup := ls.mustLookup(-1)
	require.Equal(t, resLookup.LeaseID, resGranted.LeaseID)
	require.Equal(t, resLookup.OriginalTTL, resGranted.TTL)
	require.Equal(t, resLookup.RemainingTTL, resGranted.TTL)

	_, err := ls.Lookup(ls.ctx, api.LeaseLookupRequest{LeaseID: 0})
	require.Error(ls.t, err)
}
