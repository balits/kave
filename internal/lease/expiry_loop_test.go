package lease

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

func newTestExpiryLoop(t *testing.T, lm *LeaseManager, ticker util.Ticker, isLeaderValue bool) (*ExpiryLoop, <-chan command.ResultLeaseExpire) {
	t.Helper()
	var (
		isLeader    = func() bool { return isLeaderValue }
		resultC     = make(chan command.ResultLeaseExpire)
		leadershipC = make(chan bool, 1)
		propose     = func(_ context.Context, cmd command.Command) (*command.Result, error) {
			if cmd.LeaseExpired == nil {
				panic("expected LEASE_EXPIRED cmd")
			}
			res, err := lm.ApplyExpired(*cmd.LeaseExpired)
			if err != nil {
				return nil, err
			}
			resultC <- *res
			result := command.Result{
				LeaseExpire: res,
			}
			return &result, nil
		}
	)

	t.Cleanup(func() {
		close(resultC)
	})

	return &ExpiryLoop{
		drainer:     lm,
		innerTicker: ticker,
		propose:     propose,
		isLeader:    isLeader,
		leadershipC: leadershipC,
		logger:      slog.Default(),
	}, resultC
}

func Test_ExpiryLoop_Run(t *testing.T) {
	t.Parallel()
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l1, err := lm.Grant(1, 10)
	require.NoError(t, err, "lease grant")
	fc.AdvanceSeconds(11)
	ft.FakeTick()

	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("apply result timed out after 500ms")
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount, "expected expiry loop to remove lease")
		got, err := lm.Lookup(l1.ID)
		require.Error(t, err)
		require.Nil(t, got, "expected backend to no longer contain revoked lease")
	}

	fc.AdvanceSeconds(1)

	select {
	case <-resultC:
		t.Fatal("expected expiry loop to not propose")
	case <-time.After(50 * time.Millisecond):
	}

}

func Test_ExpiryLoop_StartAsNonLeader_NoProposals(t *testing.T) {
	t.Parallel()
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)

	ex, resultC := newTestExpiryLoop(t, lm, ft, false)
	loop(t, ex)

	_, err := lm.Grant(1, 10)
	require.NoError(t, err, "lease grant")
	fc.AdvanceSeconds(11)

	select {
	case res := <-resultC:
		t.Errorf("non-leader should not propose, got result: %+v", res)
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_LosesLeadership_StopsProposing(t *testing.T) {
	t.Parallel()
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)

	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	// as leader, tick
	l1, err := lm.Grant(1, 10)
	require.NoError(t, err, "lease grant")
	fc.AdvanceSeconds(11)
	ft.FakeTick()

	select {
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount, "expected revoke to succeed as leader")
		got, err := lm.Lookup(l1.ID)
		require.Error(t, err)
		require.Nil(t, got)
	case <-time.After(50 * time.Millisecond):
		t.Errorf("timed out waiting for revoke as leader")
	}

	ex.OnLeadershipLost()
	time.Sleep(50 * time.Millisecond)

	l2, err := lm.Grant(2, 10)
	require.NoError(t, err, "lease grant")
	fc.AdvanceSeconds(11)
	go func() {
		defer func() {
			r := recover()
			t.Log(r)
		}()
		ft.FakeTick() // this should block until tickerC is not nil which will never happen
	}()

	select {
	case <-resultC:
		t.Errorf("expected no revoke after losing leadership")
	case <-time.After(50 * time.Millisecond):
	}

	got, err := lm.Lookup(l2.ID)
	require.NoError(t, err)
	require.NotNil(t, got, "expected lease to still live as no revoke shouldve been called as non leader")
}

func Test_ExpiryLoop_RegainsLeadership_ResumesProposing(t *testing.T) {
	t.Parallel()
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)

	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	// lose then regain leadership
	ex.OnLeadershipLost()
	time.Sleep(20 * time.Millisecond)
	ex.OnLeadershipGranted()
	time.Sleep(20 * time.Millisecond)

	l, err := lm.Grant(1, 10)
	require.NoError(t, err, "lease grant")
	fc.AdvanceSeconds(11)
	ft.FakeTick()

	select {
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount, "expected revoke after regaining leadership")
		got, err := lm.Lookup(l.ID)
		require.Error(t, err)
		require.Nil(t, got)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for revoke after regaining leadership")
	}
}

func Test_ExpiryLoop_NoExpiredLeases(t *testing.T) {
	t.Parallel()
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	_, err := lm.Grant(1, 3600)
	require.NoError(t, err, "lease grant")
	ft.FakeTick()

	select {
	case <-resultC:
		t.Fatal("0 expired leases should not result in a proposal to the fsm")
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_MultipleLeases_OnlyExpiredProposed(t *testing.T) {
	t.Parallel()
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	expired1, err := lm.Grant(1, 5)
	require.NoError(t, err, "lease grant")
	expired2, err := lm.Grant(2, 8)
	require.NoError(t, err, "lease grant")
	live, err := lm.Grant(3, 3600)
	require.NoError(t, err, "lease grant")

	fc.AdvanceSeconds(10)
	ft.FakeTick()

	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("timed out waiting for result")
	case <-resultC:
	}

	got, err := lm.Lookup(expired1.ID)
	require.Error(t, err)
	require.Nil(t, got, "expired1 should be revoked")

	got, err = lm.Lookup(expired2.ID)
	require.Error(t, err)
	require.Nil(t, got, "expired2 should be revoked")

	got, err = lm.Lookup(live.ID)
	require.NoError(t, err)
	require.NotNil(t, got, "live lease must not be revoked")

	select {
	case res := <-resultC:
		t.Errorf("unexpected third proposal: %+v", res)
	case <-time.After(200 * time.Millisecond):
	}
}

func Test_ExpiryLoop_KeepAlive_PreventsExpiry(t *testing.T) {
	t.Parallel()
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l, err := lm.Grant(1, 10)
	require.NoError(t, err, "lease grant")

	fc.AdvanceSeconds(9)
	_, err = lm.KeepAlive(l.ID)
	require.NoError(t, err, "lease lookup")

	fc.AdvanceSeconds(5) // past original TTL, keepalive reset it
	ft.FakeTick()

	select {
	case res := <-resultC:
		require.Equal(t, 0, res.RemovedLeaseCount)
		got, err := lm.Lookup(l.ID)
		require.NoError(t, err)
		require.NotNil(t, got)
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_ExpiredLease_ProposedOnlyOnce(t *testing.T) {
	t.Parallel()
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l, err := lm.Grant(1, 10)
	require.NoError(t, err, "lease grant")
	fc.AdvanceSeconds(11)

	ft.FakeTick()
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for first tick")
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount)
		got, err := lm.Lookup(l.ID)
		require.Error(t, err)
		require.Nil(t, got)
	}

	ft.FakeTick()
	select {
	case <-resultC:
		t.Fatal("second tick has no expired leases, still proposed")
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_ExpiryAcrossTwoTicks(t *testing.T) {
	t.Parallel()
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l1, err := lm.Grant(1, 5)
	require.NoError(t, err, "lease grant")
	l2, err := lm.Grant(2, 15)
	require.NoError(t, err, "lease grant")

	fc.AdvanceSeconds(6)
	ft.FakeTick()
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for l1 revoke")
	case <-resultC:
		got, err := lm.Lookup(l1.ID)
		require.Error(t, err)
		require.Nil(t, got, "l1 should be revoked on first tick")

		got, err = lm.Lookup(l2.ID)
		require.NoError(t, err)
		require.NotNil(t, got, "l2 should still be alive after first tick")
	}

	fc.AdvanceSeconds(10)
	ft.FakeTick()
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for l2 revoke")
	case <-resultC:
		got, err := lm.Lookup(l2.ID)
		require.Error(t, err)
		require.Nil(t, got, "l2 should be revoked on second tick")
	}
}

func Test_ExpiryLoop_EmptyManager_NoProposals(t *testing.T) {
	t.Parallel()
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	fc.AdvanceSeconds(100)
	ft.FakeTick()

	select {
	case <-resultC:
		t.Errorf("empty manager still got proposal after no lease couldve expired")
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_DoubleStart_Idempotent(t *testing.T) {
	t.Parallel()
	var (
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = newTestLm(t)
	)
	ex, _ := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		ex.Run(t.Context())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Error("second Run() did not return quickly — atomic guard may be broken")
	}
}

func loop(t *testing.T, ex *ExpiryLoop) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go ex.Run(ctx)
}
