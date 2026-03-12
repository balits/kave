package lease

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
)

const minCheckpointInterval = time.Second * 30

type CheckpointScheduler struct {
	lm       *LeaseManager
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	running  atomic.Bool
	propose  util.ProposeFunc
	isLeader util.IsLeaderFunc
	logger   *slog.Logger
}

func NewCheckpointScheduler(logger *slog.Logger, lm *LeaseManager, interval time.Duration, propose util.ProposeFunc, isLeaser util.IsLeaderFunc) *CheckpointScheduler {
	if interval <= 0 {
		interval = minCheckpointInterval
	}
	return &CheckpointScheduler{
		lm:       lm,
		interval: interval,
		propose:  propose,
		isLeader: isLeaser,
		logger:   logger.With("component", "checkpoint_scheduler"),
	}
}
func (cs *CheckpointScheduler) Run(ctx context.Context) {
	if !cs.running.CompareAndSwap(false, true) {
		cs.logger.Warn("Attempted to run checkpoint scheduler that was already running")
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	cs.ctx = ctx
	cs.cancel = cancel
	cs.logger.Info("Checkpoint scheduler started")
	cs.run()
}

func (cs *CheckpointScheduler) run() {
	ticker := time.NewTicker(cs.interval)
	for {
		select {
		case <-ticker.C:
			cs.tick()
		case <-cs.ctx.Done():
			// one last tick to flush out all the info we have,
			// then let the next leader do its thing
			cs.logger.Info("Stopping CheckpointScheduler, running one last tick")
			cs.tick()
			return
		}
	}
}

func (cs *CheckpointScheduler) tick() {
	if !cs.isLeader() {
		// do nothing
		// TODO: maybe use raft.leaderCh in the future (but even that can be faulty)
		return
	}

	subcmd := cs.lm.genereteCheckpoints()
	if len(subcmd.Checkpoints) == 0 {
		cs.logger.Info("checkpoint: no live leases, skipping tick")
		return
	}

	cmd := command.Command{
		Type:            command.CmdLeaseCheckpoint,
		LeaseCheckpoint: &subcmd,
	}

	applyFut, err := cs.propose(cmd)
	if err != nil {
		cs.logger.Warn("checkpoint: failed to propose checkpoints",
			"error", err,
		)
		return
	}
	_, err = util.WaitApply(cs.ctx, applyFut)
	if err != nil {
		cs.logger.Warn("checkpoint: failed to await apply response",
			"error", err,
		)
		return
	}
	cs.lm.metrics.CheckpointsTotal.Inc()
}

func (cs *CheckpointScheduler) Stop() {
	cs.logger.Info("Stopping checkpoint scheduler")
	cs.cancel()
	cs.running.Store(false)
}
