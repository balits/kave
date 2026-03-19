package lease

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

// checkpointPropose is a recording ProposeFunc for checkpoint tests.
type checkpointPropose struct {
	cmds  []command.Command
	errOn int // if > 0, return an error on the Nth call (1-indexed)
	calls int
}

func (cp *checkpointPropose) proposeFunc() util.ProposeFunc {
	return func(_ context.Context, cmd command.Command) (*command.Result, error) {
		cp.calls++
		cp.cmds = append(cp.cmds, cmd)
		if cp.errOn > 0 && cp.calls == cp.errOn {
			return nil, errors.New("simulated propose error")
		}
		return &command.Result{}, nil
	}
}

func (cp *checkpointPropose) lastCmd() *command.Command {
	if len(cp.cmds) == 0 {
		return nil
	}
	c := cp.cmds[len(cp.cmds)-1]
	return &c
}

func (cp *checkpointPropose) totalCalls() int { return cp.calls }

func newTestScheduler(t *testing.T, lm *LeaseManager, isLeaderValue bool) (*CheckpointScheduler, *checkpointPropose) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	cp := &checkpointPropose{}
	cs := &CheckpointScheduler{
		lm:          lm,
		interval:    minCheckpointInterval,
		propose:     cp.proposeFunc(),
		isLeader:    func() bool { return isLeaderValue },
		leadershipC: make(chan bool, 1),
		logger:      slog.Default(),
		ctx:         ctx,
		cancel:      cancel,
	}
	return cs, cp
}

func Test_CheckpointScheduler_NonLeader_NoProposal(t *testing.T) {
	lm := newTestLm(t)
	cs, cp := newTestScheduler(t, lm, false)

	lm.Grant(0, 30)
	lm.Grant(0, 60)

	cs.tick()

	require.Equal(t, 0, cp.totalCalls(), "non-leader must not propose checkpoints")
}

func Test_CheckpointScheduler_EmptyManager_NoProposal(t *testing.T) {
	lm := newTestLm(t)
	cs, cp := newTestScheduler(t, lm, true)

	cs.tick()

	require.Equal(t, 0, cp.totalCalls(), "no proposal when there are no leases")
}

func Test_CheckpointScheduler_LiveLeases_ProposedWithCorrectTTL(t *testing.T) {
	now := time.Now()
	fc := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, fc)
	cs, cp := newTestScheduler(t, lm, true)

	l1, _ := lm.Grant(1, 30)
	l2, _ := lm.Grant(2, 60)
	l3, _ := lm.Grant(3, 90)

	cs.tick()

	require.Equal(t, 1, cp.totalCalls(), "expected exactly one propose call")
	cmd := cp.lastCmd()
	require.NotNil(t, cmd)
	require.NotNil(t, cmd.LeaseCheckpoint, "proposed command must be a checkpoint")
	require.Equal(t, command.KindLeaseCheckpoint, cmd.Kind)

	entries := cmd.LeaseCheckpoint.Checkpoints
	require.Len(t, entries, 3)

	byID := make(map[int64]command.Checkpoint)
	for _, e := range entries {
		byID[e.LeaseID] = e
	}

	for _, l := range []*Lease{l1, l2, l3} {
		e, ok := byID[l.ID]
		require.True(t, ok, "lease %d should appear in checkpoint", l.ID)
		require.Equal(t, l.TTL, e.RemainingTTL,
			"remaining TTL should equal full TTL when no time has passed for lease %d", l.ID)
	}
}

func Test_CheckpointScheduler_AdvancedClock_DecreasesTTL(t *testing.T) {
	now := time.Now()
	fc := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, fc)
	cs, cp := newTestScheduler(t, lm, true)

	lm.Grant(1, 60)
	lm.Grant(2, 120)

	fc.AdvanceSeconds(20)
	cs.tick()

	require.Equal(t, 1, cp.totalCalls())
	entries := cp.lastCmd().LeaseCheckpoint.Checkpoints
	require.Len(t, entries, 2)

	for _, e := range entries {
		var expectedRemaining int64
		switch e.LeaseID {
		case 1:
			expectedRemaining = 40 // 60 - 20
		case 2:
			expectedRemaining = 100 // 120 - 20
		}
		require.Equal(t, expectedRemaining, e.RemainingTTL,
			"lease %d: remaining TTL should be TTL - elapsed", e.LeaseID)
	}
}

func Test_CheckpointScheduler_ExpiredLeases_Excluded(t *testing.T) {
	now := time.Now()
	fc := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, fc)
	cs, cp := newTestScheduler(t, lm, true)

	lm.Grant(1, 5)  // will be expired after advance
	lm.Grant(2, 60) // still live

	fc.AdvanceSeconds(10) // past lease 1's TTL
	cs.tick()

	require.Equal(t, 1, cp.totalCalls())
	entries := cp.lastCmd().LeaseCheckpoint.Checkpoints

	// Only lease 2 should appear — lease 1 has rem <= 0.
	require.Len(t, entries, 1, "only live lease should appear in checkpoint")
	require.Equal(t, int64(2), entries[0].LeaseID, "lease 2 should be checkpointed")
	require.Greater(t, entries[0].RemainingTTL, int64(0))
}

func Test_CheckpointScheduler_AllExpired_NoProposal(t *testing.T) {
	now := time.Now()
	fc := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, fc)
	cs, cp := newTestScheduler(t, lm, true)

	lm.Grant(1, 5)
	lm.Grant(2, 8)

	fc.AdvanceSeconds(10)
	cs.tick()

	require.Equal(t, 0, cp.totalCalls(), "no proposal when all leases are expired")
}

func Test_CheckpointScheduler_ProposeError_IsNonFatal(t *testing.T) {
	lm := newTestLm(t)
	cs, cp := newTestScheduler(t, lm, true)
	cp.errOn = 1

	lm.Grant(0, 60)

	cs.tick()
	require.Equal(t, 1, cp.totalCalls(), "first tick should have attempted propose")

	cs.tick()
	require.Equal(t, 2, cp.totalCalls(), "second tick should attempt propose again")

	cmd := cp.lastCmd()
	require.NotNil(t, cmd.LeaseCheckpoint)
	require.Greater(t, len(cmd.LeaseCheckpoint.Checkpoints), 0)
}

func Test_CheckpointScheduler_MultipleTicks_AccumulatesCorrectly(t *testing.T) {
	now := time.Now()
	fc := util.NewFakeClock(now).(*util.FakeClock)
	lm := lmWithClock(t, fc)
	cs, cp := newTestScheduler(t, lm, true)

	lm.Grant(1, 100)

	cs.tick()
	require.Equal(t, 1, cp.totalCalls())
	require.Equal(t, int64(100), cp.lastCmd().LeaseCheckpoint.Checkpoints[0].RemainingTTL)

	fc.AdvanceSeconds(10)
	cs.tick()
	require.Equal(t, 2, cp.totalCalls())
	require.Equal(t, int64(90), cp.lastCmd().LeaseCheckpoint.Checkpoints[0].RemainingTTL)

	fc.AdvanceSeconds(20)
	cs.tick()
	require.Equal(t, 3, cp.totalCalls())
	require.Equal(t, int64(70), cp.lastCmd().LeaseCheckpoint.Checkpoints[0].RemainingTTL)
}
