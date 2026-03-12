package compactor

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

type windowRetentionCompactor struct {
	kind        CompactorKind
	window      int64
	threshold   int64
	compactable Compactable
	paused      atomic.Bool
	running     atomic.Bool
	ctx         context.Context
	cancel      context.CancelFunc
	interval    time.Duration
	logger      *slog.Logger
}

func newWindowRetentionCompactor(logger *slog.Logger, compactable Compactable, opts CompactorOptions) Compactor {
	return &windowRetentionCompactor{
		kind:        opts.Kind,
		window:      opts.WindowSize,
		threshold:   opts.Threshold,
		compactable: compactable,
		interval:    opts.PollInterval,
		logger: logger.With(
			"component", "compactor",
			"compactor_kind", string(opts.Kind),
		),
	}
}

// Run blocks to current thread, so it should be called on its own goroutine
func (wc *windowRetentionCompactor) Run(ctx context.Context) {
	if !wc.running.CompareAndSwap(false, true) {
		wc.logger.Warn("Attempted to run compactor that was already running")
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	wc.ctx = ctx
	wc.cancel = cancel
	wc.logger.Info("Compactor started")
	wc.run()
}

func (wc *windowRetentionCompactor) run() {
	// TODO: ticks could pile up if compactions are slow (<-doneC blcoks)
	ticker := time.NewTicker(wc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-wc.ctx.Done():
			wc.running.Store(false)
			return
		case <-ticker.C:
		}
		if wc.paused.Load() {
			continue
		}

		currentRev, compactedRev := wc.compactable.Revisions()
		targetRev := currentRev.Main - wc.window

		if targetRev <= compactedRev {
			continue
		}
		if targetRev-compactedRev < wc.threshold {
			continue
		}
		wc.logger.Debug("Starting compaction",
			"current_rev", currentRev.Main,
			"compacted_rev", compactedRev,
			"target_rev", targetRev,
		)
		doneC, err := wc.compactable.Compact(targetRev)
		if err != nil {
			wc.logger.Error("Failed to start compaction",
				"error", err,
			)
			continue
		}
		wc.logger.Debug("Compaction started")

		<-doneC
		wc.logger.Debug("Compaction finished")
	}
}

func (wc *windowRetentionCompactor) Pause() {
	wc.paused.Store(true)
}

func (wc *windowRetentionCompactor) Resume() {
	wc.paused.Store(false)
}

func (wc *windowRetentionCompactor) Stop() {
	wc.logger.Info("Stopping compactor")
	wc.cancel()
	wc.paused.Store(false) // so that future Runs can continue
	wc.running.Store(false)
}
