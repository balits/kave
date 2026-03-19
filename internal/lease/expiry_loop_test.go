package lease

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

func newTestExpiryLoop(t *testing.T, lm *LeaseManager, ticker util.Ticker, isLeaderValue bool) (*ExpiryLoop, <-chan command.LeaseExpireResult) {
	t.Helper()
	var (
		isLeader    = func() bool { return isLeaderValue }
		resultC     = make(chan command.LeaseExpireResult)
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
				LeaseExpireResult: res,
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
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l1, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)
	ft.Tick()

	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("apply result timed out after 500ms")
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount, "expected expiry loop to remove lease")
		require.Nil(t, lm.Lookup(l1.ID), "expected backend to no longer contain revoked lease")
	}

	fc.AdvanceSeconds(1)

	select {
	case <-resultC:
		t.Fatal("expected expiry loop to not propose")
	case <-time.After(50 * time.Millisecond):
	}

}

func Test_ExpiryLoop_StartAsNonLeader_NoProposals(t *testing.T) {
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)

	ex, resultC := newTestExpiryLoop(t, lm, ft, false)
	loop(t, ex)

	lm.Grant(0, 10)
	fc.AdvanceSeconds(11)

	select {
	case res := <-resultC:
		t.Errorf("non-leader should not propose, got result: %+v", res)
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_LosesLeadership_StopsProposing(t *testing.T) {
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)

	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	// as leader, tick
	l1, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)
	ft.Tick()

	select {
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount, "expected revoke to succeed as leader")
		require.Nil(t, lm.Lookup(l1.ID))
	case <-time.After(50 * time.Millisecond):
		t.Errorf("timed out waiting for revoke as leader")
	}

	ex.OnLeadershipLost()
	time.Sleep(50 * time.Millisecond)

	l2, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)
	go func() {
		defer func() {
			r := recover()
			fmt.Println(r)
		}()
		ft.Tick() // this should block until tickerC is not nil which will never happen
	}()

	select {
	case <-resultC:
		t.Errorf("expected no revoke after losing leadership")
	case <-time.After(50 * time.Millisecond):
	}
	require.NotNil(t, lm.Lookup(l2.ID), "expected lease to still live as no revoke shouldve been called as non leader")
}

func Test_ExpiryLoop_RegainsLeadership_ResumesProposing(t *testing.T) {
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

	l, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)
	ft.Tick()

	select {
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount, "expected revoke after regaining leadership")
		require.Nil(t, lm.Lookup(l.ID))
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for revoke after regaining leadership")
	}
}

func Test_ExpiryLoop_NoExpiredLeases(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	lm.Grant(0, 3600)
	ft.Tick()

	select {
	case res := <-resultC:
		require.Equal(t, 0, res.RemovedLeaseCount)
		require.Equal(t, 0, res.RemovedKeyCount)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("expected empty proposal")
	}
}

func Test_ExpiryLoop_MultipleLeases_OnlyExpiredProposed(t *testing.T) {
	fc := util.NewFakeClock(time.Now()).(*util.FakeClock)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	lm := lmWithClock(t, fc)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	expired1, _ := lm.Grant(0, 5)
	expired2, _ := lm.Grant(0, 8)
	live, _ := lm.Grant(0, 3600)

	fc.AdvanceSeconds(10)
	ft.Tick()

	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("timed out waiting for result")
	case <-resultC:
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

func Test_ExpiryLoop_KeepAlive_PreventsExpiry(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l, _ := lm.Grant(0, 10)

	fc.AdvanceSeconds(9)
	_, err := lm.KeepAlive(l.ID)
	require.NoError(t, err)

	fc.AdvanceSeconds(5) // past original TTL, keepalive reset it
	ft.Tick()

	select {
	case res := <-resultC:
		require.Equal(t, 0, res.RemovedLeaseCount)
		require.NotNil(t, lm.Lookup(l.ID))
	case <-time.After(50 * time.Millisecond):
	}
}

func Test_ExpiryLoop_ExpiredLease_ProposedOnlyOnce(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l, _ := lm.Grant(0, 10)
	fc.AdvanceSeconds(11)

	ft.Tick()
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for first tick")
	case res := <-resultC:
		require.Equal(t, 1, res.RemovedLeaseCount)
		require.Nil(t, lm.Lookup(l.ID))
	}

	ft.Tick()
	select {
	case res := <-resultC:
		require.Equal(t, 0, res.RemovedLeaseCount)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for second tick")
	}
}

func Test_ExpiryLoop_ExpiryAcrossTwoTicks(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	l1, _ := lm.Grant(0, 5)
	l2, _ := lm.Grant(0, 15)

	fc.AdvanceSeconds(6)
	ft.Tick()
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for l1 revoke")
	case <-resultC:
		require.Nil(t, lm.Lookup(l1.ID), "l1 should be revoked on first tick")
		require.NotNil(t, lm.Lookup(l2.ID), "l2 should still be alive after first tick")
	}

	fc.AdvanceSeconds(10)
	ft.Tick()
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for l2 revoke")
	case <-resultC:
		require.Nil(t, lm.Lookup(l2.ID), "l2 should be revoked on second tick")
	}
}

func Test_ExpiryLoop_EmptyManager_NoProposals(t *testing.T) {
	var (
		fc = util.NewFakeClock(time.Now()).(*util.FakeClock)
		ft = util.NewFakeTicker().(*util.FakeTicker)
		lm = lmWithClock(t, fc)
	)
	ex, resultC := newTestExpiryLoop(t, lm, ft, true)
	loop(t, ex)

	fc.AdvanceSeconds(100)
	ft.Tick()

	select {
	case res := <-resultC:
		require.Equal(t, 0, res.RemovedLeaseCount)
	case <-time.After(50 * time.Millisecond):
		t.Errorf("timed out waiting for proposal")
	}
}

func Test_ExpiryLoop_DoubleStart_Idempotent(t *testing.T) {
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
