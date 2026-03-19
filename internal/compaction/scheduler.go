package compaction

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/util"
)

const (
	DefaultThreshold   int64 = 10
	DefaultIntervalMin int64 = 30
)

type Options struct {
	Threshold   int64
	IntervalMin int64
}

func (co *Options) Validate() error {
	if co.Threshold <= 0 {
		return errors.New("compactor threshold must be positive")
	}
	if co.IntervalMin <= 0 {
		return errors.New("compactor interval_min must be positive")
	}
	return nil
}

type CompactionScheduler struct {
	threshold    int64
	store        mvcc.SmartRevisionGetter
	ticker       util.Ticker
	running      atomic.Bool
	propose      util.ProposeFunc
	isLeader     util.IsLeaderFunc
	candidateRev int64
	leadershipC  chan bool
	ctx          context.Context
	cancel       context.CancelFunc
	logger       *slog.Logger
}

func NewScheduler(logger *slog.Logger, store mvcc.SmartRevisionGetter, propose util.ProposeFunc, isLeader util.IsLeaderFunc, opts *Options) *CompactionScheduler {
	var threshold, intervalMin int64

	if opts != nil {
		threshold = opts.Threshold
		intervalMin = opts.IntervalMin
	} else {
		threshold = DefaultThreshold
		intervalMin = DefaultIntervalMin
	}

	return &CompactionScheduler{
		threshold: threshold,
		store:     store,
		ticker:    util.NewRealTicker(time.Duration(intervalMin) * time.Minute),
		propose:   propose,
		isLeader:  isLeader,
		// buffer of one, then use drain-then-send:
		// we dont want to block the observer with stale values
		// so we drain before sending the latest
		leadershipC: make(chan bool, 1),
		logger:      logger.With("component", "compation_scheduler"),
	}
}

func (cs *CompactionScheduler) OnLeadershipGranted() {
	select {
	case <-cs.leadershipC:
	default:
	}
	cs.leadershipC <- true
}

func (cs *CompactionScheduler) OnLeadershipLost() {
	select {
	case <-cs.leadershipC:
	default:
	}
	cs.leadershipC <- false
}

func (cs *CompactionScheduler) Run(ctx context.Context) {
	if !cs.running.CompareAndSwap(false, true) {
		cs.logger.Warn("Attempted to run compaction scheduler while it was already running")
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	cs.ctx = ctx
	cs.cancel = cancel
	cs.logger.Info("Compaction scheduler started")
	cs.run()
}

func (cs *CompactionScheduler) run() {
	defer cs.ticker.Stop()
	var tickerC <-chan time.Time

	if cs.isLeader() {
		cs.logger.Info("Already leader, setting up ticker channel")
		tickerC = cs.ticker.C()
	}

	for {
		select {
		case granted, ok := <-cs.leadershipC:
			if !ok {
				cs.logger.Error("leadership channel closed, stopping main loop")
				return
			}
			if granted && cs.isLeader() {
				tickerC = cs.ticker.C()
			} else {
				tickerC = nil
			}
		case <-cs.ctx.Done():
			cs.logger.Info("context cancelled, stopping main loop", "cause", cs.ctx.Err())
			return
		case <-tickerC:
			cs.tick()
		}
	}
}

func (cs *CompactionScheduler) tick() {
	currentRev, lastCompacted := cs.store.Revisions()
	prev := cs.candidateRev
	cs.candidateRev = currentRev.Main

	if cs.candidateRev < 0 {
		cs.logger.Debug("first tick: setting candidateRev, skipping compaction")
		return
	}

	if currentRev.Main-lastCompacted < cs.threshold {
		cs.logger.Info("compaction threshold not met, continuing",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
			"threshold", cs.threshold,
		)
		return
	}

	cmd := command.Command{
		Kind: command.KindCompact,
		Compact: &command.CompactCmd{
			TargetRev: prev,
		},
	}

	result, err := cs.propose(cs.ctx, cmd)
	if err != nil {
		cs.logger.Warn("compaction error: failed to propose compaction",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
			"error", err,
		)
		return
	}

	if result.Error != nil {
		cs.logger.Warn("compaction error",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
			"error", result.Error,
		)
		return
	}

	if result.Compact == nil {
		cs.logger.Error("compaction error: compact result was nil",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
		)
		return
	}

	if result.Compact.Err != nil {
		cs.logger.Warn("compaction error",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
			"error", result.Compact.Err,
		)
		return
	}

	select {
	case <-result.Compact.DoneC:
		cs.logger.Info("compaction finished",
			"current_rev", currentRev.Main,
			"new_compacted_rev", prev,
			"last_compacted_rev", lastCompacted,
		)
	case <-cs.ctx.Done():
		cs.logger.Info("context cancelled while waiting for compaction to finish",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
		)
	}
}

func (cs *CompactionScheduler) Stop() {
	cs.logger.Info("Stopping compactor")
	cs.cancel()
	cs.running.Store(false)
}
