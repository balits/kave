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
	lm          *LeaseManager
	interval    time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	running     atomic.Bool
	propose     util.ProposeFunc
	isLeader    util.IsLeaderFunc
	leadershipC chan bool
	logger      *slog.Logger
}

func NewCheckpointScheduler(logger *slog.Logger, lm *LeaseManager, interval time.Duration, propose util.ProposeFunc, isLeader util.IsLeaderFunc) *CheckpointScheduler {
	if interval <= 0 {
		interval = minCheckpointInterval
	}
	return &CheckpointScheduler{
		lm:       lm,
		interval: interval,
		propose:  propose,
		isLeader: isLeader,
		// buffer of one, then use drain-then-send:
		// we dont want to block the observer with stale values
		// so we drain before sending the latest
		leadershipC: make(chan bool, 1),
		logger:      logger.With("component", "checkpoint_scheduler"),
	}
}

// drain-then-send, to prevent stale values
func (cs *CheckpointScheduler) OnLeadershipGranted() {
	select {
	case <-cs.leadershipC:
	default:
	}
	cs.leadershipC <- true
}

// drain-then-send, to prevent stale values
func (cs *CheckpointScheduler) OnLeadershipLost() {
	select {
	case <-cs.leadershipC:
	default:
	}
	cs.leadershipC <- false
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
	var ticker *time.Ticker
	var tickerC <-chan time.Time

	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	// initial check (if the channel didnt caught up to any leadership updates)
	if cs.isLeader() {
		ticker = time.NewTicker(cs.interval)
		tickerC = ticker.C
	}

	for {
		select {
		case granted, ok := <-cs.leadershipC:
			if !ok {
				return
			}
			if granted && cs.isLeader() {
				ticker = time.NewTicker(cs.interval)
				tickerC = ticker.C
			} else {
				ticker.Stop()
				ticker = nil
				tickerC = nil
			}
		case <-cs.ctx.Done():
			cs.logger.Info("context cancelled, stopping CheckpointScheduler",
				"cause", cs.ctx.Err(),
			)
			return
		case <-tickerC:
			cs.tick()
		}
	}
}

func (cs *CheckpointScheduler) tick() {
	// double check on isLeader, usefull in tests, but should be refactored away
	if !cs.isLeader() {
		return
	}

	cps := cs.lm.Checkpoint()
	if len(cps) == 0 {
		cs.logger.Info("checkpoint: no live leases, skipping tick")
		return
	}

	cmd := command.Command{
		Type: command.CmdLeaseCheckpoint,
		LeaseCheckpoint: &command.LeaseCheckpointCmd{
			Checkpoints: cps,
		},
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
