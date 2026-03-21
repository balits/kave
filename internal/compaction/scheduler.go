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
	DefaultThreshold   = 10
	DefaultIntervalMin = 30
	DefaultMaxRevGap   = 10
)

type Options struct {
	Threshold   int64
	IntervalMin int64
	MaxRevGap   int64
}

var DefaultOptions = Options{
	Threshold:   DefaultThreshold,
	IntervalMin: DefaultIntervalMin,
	MaxRevGap:   DefaultMaxRevGap,
}

func (co *Options) Validate() error {
	if co.Threshold <= 0 {
		return errors.New("compactor threshold must be positive")
	}
	if co.IntervalMin <= 0 {
		return errors.New("compactor interval_min must be positive")
	}
	if co.MaxRevGap <= 0 {
		return errors.New("compactor max_rev_gap must be positive")
	}
	return nil
}

type CompactionScheduler struct {
	opts           Options
	store          mvcc.SmartRevisionGetter
	ticker         util.Ticker
	running        atomic.Bool
	propose        util.ProposeFunc
	isLeader       util.IsLeaderFunc
	candidateRev   int64
	leadershipC    chan bool
	writePressureC chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *slog.Logger
}

func NewScheduler(logger *slog.Logger, store mvcc.SmartRevisionGetter, propose util.ProposeFunc, isLeader util.IsLeaderFunc, opts *Options) *CompactionScheduler {
	var o Options
	if opts != nil {
		o = *opts
	} else {
		o = DefaultOptions
	}

	return &CompactionScheduler{
		opts:     o,
		store:    store,
		ticker:   util.NewRealTicker(time.Duration(o.IntervalMin) * time.Minute),
		propose:  propose,
		isLeader: isLeader,
		// buffer of one, then use drain-then-send:
		// we dont want to block the observer with stale values
		// so we drain before sending the latest
		leadershipC:    make(chan bool, 1),
		writePressureC: make(chan struct{}, 1),
		logger:         logger.With("component", "compation_scheduler"),
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

func (cs *CompactionScheduler) OnWrite(currentRev int64) {
	_, compactRev := cs.store.Revisions()
	if currentRev-compactRev >= cs.opts.MaxRevGap {
		select {
		case cs.writePressureC <- struct{}{}:
		default:
		}
	}
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
		case <-cs.writePressureC:
			if tickerC != nil {
				cs.tick(true)
			}
		case <-tickerC:
			cs.tick(false)
		}
	}
}

func (cs *CompactionScheduler) tick(force bool) {
	currentRev, lastCompacted := cs.store.Revisions()
	prev := cs.candidateRev
	cs.candidateRev = currentRev.Main

	if cs.candidateRev < 0 {
		cs.logger.Debug("first tick: setting candidateRev, skipping compaction")
		return
	}

	if !force && currentRev.Main-lastCompacted < cs.opts.Threshold {
		cs.logger.Info("compaction threshold not met, continuing",
			"current_rev", currentRev.Main,
			"last_compacted_rev", lastCompacted,
			"threshold", cs.opts.Threshold,
		)
		return
	}

	cmd := command.Command{
		Kind: command.KindCompact,
		Compact: &command.CompactCmd{
			TargetRev: prev,
		},
	}

	l := cs.logger.With(
		"current_rev", currentRev.Main,
		"last_compacted_rev", lastCompacted,
		"candidate_rev", cmd.Compact.TargetRev,
	)

	result, err := cs.propose(cs.ctx, cmd)
	if err != nil {
		l.Warn("compaction error: failed to propose compaction", "error", err)
		return
	}

	if result.Error != nil {
		l.Warn("compaction error", "error", result.Error)
		return
	}

	if result.Compact == nil {
		l.Error("compaction error: compact result was nil")
		return
	}

	if result.Compact.Error != nil {
		l.Warn("compaction error", "error", result.Compact.Error)
		return
	}

	select {
	case <-result.Compact.DoneC:
		l.Info("compaction finished")
	case <-cs.ctx.Done():
		l.Info("context cancelled while waiting for compaction to finish")
	}
}

func (cs *CompactionScheduler) Stop() {
	cs.logger.Info("Stopping compactor")
	cs.cancel()
	cs.running.Store(false)
}
