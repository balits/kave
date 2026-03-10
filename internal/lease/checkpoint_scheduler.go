package lease

import (
	"context"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
)

const minCheckpointInterval = time.Second * 30

type CheckpointScheduler struct {
	lm       *LeaseManager
	interval time.Duration
	propose  util.ProposeFunc
	isLeader util.IsLeaderFunc
	logger   *slog.Logger
}

func NewCheckpointScheduler(logger *slog.Logger, lm *LeaseManager, interval time.Duration) *CheckpointScheduler {
	if interval <= 0 {
		interval = minCheckpointInterval
	}
	return &CheckpointScheduler{
		lm:       lm,
		interval: interval,
		logger:   logger.With("component", "checkpoint_scheduler"),
	}
}

func (cs *CheckpointScheduler) InjectProposeFunc(f util.ProposeFunc) {
	cs.propose = f
}

func (cs *CheckpointScheduler) InjectIsLeaderFunc(f util.IsLeaderFunc) {
	cs.isLeader = f
}

func (cs *CheckpointScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(cs.interval)
	cs.logger.Info("Starting CheckpointScheduler")
	for {
		select {
		case <-ticker.C:
			cs.tick(ctx)
		case <-ctx.Done():
			// one last tick to flush out all the info we have,
			// then let the next leader do its thing
			cs.logger.Info("Stopping CheckpointScheduler, running one last tick")
			cs.tick(context.TODO())
			return
		}
	}
}

func (cs *CheckpointScheduler) tick(ctx context.Context) {
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
	_, err = util.WaitApply(ctx, applyFut)
	if err != nil {
		cs.logger.Warn("checkpoint: failed to await apply response",
			"error", err,
		)
		return
	}
	cs.lm.metrics.CheckpointsTotal.Inc()
}
