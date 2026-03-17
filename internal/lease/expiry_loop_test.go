package lease

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func newTestExpiryLoop(t *testing.T, lm *LeaseManager, ticker util.Ticker) (*ExpiryLoop, <-chan command.LeaseRevokeResult) {
	t.Helper()
	var (
		isLeader = func() bool { return true }
		resultC  = make(chan command.LeaseRevokeResult)
		propose  = func(cmd command.Command) (raft.ApplyFuture, error) {
			if cmd.LeaseRevoke == nil {
				panic("expected LEASE_REVOKE cmd")
			}
			found, revoked := lm.Revoke(cmd.LeaseRevoke.LeaseID)
			result := command.LeaseRevokeResult{
				Found:   found,
				Revoked: revoked,
			}
			resultC <- result
			return &expiryFuture{
				result: result,
			}, nil
		}
	)

	t.Cleanup(func() {
		close(resultC)
	})

	return &ExpiryLoop{
		drainer:  lm,
		ticker:   ticker,
		propose:  propose,
		isLeader: isLeader,
		logger:   slog.Default(),
	}, resultC
}

func Test_ExpiryLoop_Run(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	l1, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)
	ft.Tick()

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("apply result timed out after 500ms")
	case res := <-resultC:
		require.True(t, res.Found, "expected expiry loop's revoke command to find lease")
		require.True(t, res.Revoked, "expected expiry loop's revoke command to revoke lease")
		require.Nil(t, lm.Lookup(l1.ID), "expected backend to no longer contain revoked lease")
	}

	fc.AdvanceSeconds(1)

	select {
	case <-resultC:
		t.Fatal("expected expiry loop to not propose")
	case <-time.After(500 * time.Millisecond):
	}

}

func Test_ExpiryLoop_NonLeader_NoProposals(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	ex.isLeader = func() bool { return false }
	loop(t, ex)

	lm.Grant(0, 10)
	fc.AdvanceSeconds(11)
	ft.Tick()

	select {
	case res := <-resultC:
		t.Errorf("non-leader should not propose, got result: %+v", res)
	case <-time.After(500 * time.Millisecond):
	}
}

// ── no expired leases ─────────────────────────────────────────────────────────

func Test_ExpiryLoop_NoExpiredLeases_NoProposals(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	lm.Grant(0, 3600)
	ft.Tick()

	select {
	case res := <-resultC:
		t.Errorf("live lease should not be proposed, got: %+v", res)
	case <-time.After(500 * time.Millisecond):
	}
}

// ── multiple leases, only expired ones revoked ────────────────────────────────

func Test_ExpiryLoop_MultipleLeases_OnlyExpiredProposed(t *testing.T) {
	var (
		fc      = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft      = util.NewFakeTicker().(*util.FakeTicker)
		lm      = lmWithClock(t, fc)
		resultC = make(chan command.LeaseRevokeResult, 10)
	)

	propose := func(cmd command.Command) (raft.ApplyFuture, error) {
		require.NotNil(t, cmd.LeaseRevoke)
		found, revoked := lm.Revoke(cmd.LeaseRevoke.LeaseID)
		result := command.LeaseRevokeResult{Found: found, Revoked: revoked}
		resultC <- result
		return &expiryFuture{result: result}, nil
	}
	t.Cleanup(func() { close(resultC) })

	ex := &ExpiryLoop{
		drainer:  lm,
		ticker:   ft,
		propose:  propose,
		isLeader: func() bool { return true },
		logger:   slog.Default(),
	}
	loop(t, ex)

	expired1, _ := lm.Grant(0, 5)
	expired2, _ := lm.Grant(0, 8)
	live, _ := lm.Grant(0, 3600)

	fc.AdvanceSeconds(10)
	ft.Tick()

	for i := 0; i < 2; i++ {
		select {
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for result %d/2", i+1)
		case <-resultC:
		}
	}

	require.Nil(t, lm.Lookup(expired1.ID), "expired1 should be revoked")
	require.Nil(t, lm.Lookup(expired2.ID), "expired2 should be revoked")
	require.NotNil(t, lm.Lookup(live.ID), "live lease must not be revoked")

	select {
	case res := <-resultC:
		t.Errorf("unexpected third proposal: %+v", res)
	case <-time.After(200 * time.Millisecond):
	}
}

// ── keepalive prevents expiry ─────────────────────────────────────────────────

func Test_ExpiryLoop_KeepAlive_PreventsExpiry(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	l, _ := lm.Grant(0, 10)

	fc.AdvanceSeconds(9)
	_, err := lm.KeepAlive(l.ID)
	require.NoError(t, err)

	fc.AdvanceSeconds(5) // past original TTL, keepalive reset it
	ft.Tick()

	select {
	case res := <-resultC:
		t.Errorf("kept-alive lease must not be proposed, got: %+v", res)
	case <-time.After(500 * time.Millisecond):
	}
}

// ── proposed only once ────────────────────────────────────────────────────────

func Test_ExpiryLoop_ExpiredLease_ProposedOnlyOnce(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	l, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)

	ft.Tick()
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first revoke")
	case res := <-resultC:
		require.True(t, res.Found)
		require.True(t, res.Revoked)
		require.Nil(t, lm.Lookup(l.ID))
	}

	ft.Tick()
	select {
	case res := <-resultC:
		t.Errorf("lease should only be proposed once, got second result: %+v", res)
	case <-time.After(500 * time.Millisecond):
	}
}

// ── expiry across two ticks ───────────────────────────────────────────────────

func Test_ExpiryLoop_ExpiryAcrossTwoTicks(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	l1, _ := lm.Grant(0, 5)
	l2, _ := lm.Grant(0, 15)

	fc.AdvanceSeconds(6)
	ft.Tick()
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for l1 revoke")
	case <-resultC:
		require.Nil(t, lm.Lookup(l1.ID), "l1 should be revoked on first tick")
		require.NotNil(t, lm.Lookup(l2.ID), "l2 should still be alive after first tick")
	}

	fc.AdvanceSeconds(10)
	ft.Tick()
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for l2 revoke")
	case <-resultC:
		require.Nil(t, lm.Lookup(l2.ID), "l2 should be revoked on second tick")
	}
}

// ── empty manager ─────────────────────────────────────────────────────────────

func Test_ExpiryLoop_EmptyManager_NoProposals(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	fc.AdvanceSeconds(100)
	ft.Tick()

	select {
	case res := <-resultC:
		t.Errorf("empty manager should produce no proposals, got: %+v", res)
	case <-time.After(500 * time.Millisecond):
	}
}

// ── double start is idempotent ────────────────────────────────────────────────

func Test_ExpiryLoop_DoubleStart_Idempotent(t *testing.T) {
	var (
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = newTestLm(t)
	)
	ex, _ := newTestExpiryLoop(t, lm, ft)
	loop(t, ex)

	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		ex.Run(t.Context())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Error("second Run() did not return quickly — atomic guard may be broken")
	}
}

func loop(t *testing.T, ex *ExpiryLoop) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go ex.Run(ctx)
}

type expiryFuture struct {
	result command.LeaseRevokeResult
}

func (f *expiryFuture) Error() error {
	return nil
}

func (f *expiryFuture) Index() uint64 {
	return 0
}

func (f *expiryFuture) Response() any {
	return f.result
}
