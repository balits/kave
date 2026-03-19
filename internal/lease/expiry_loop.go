package lease

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/util"
)

const expiryLoopInterval = 500 * time.Millisecond

type ExpiredLeaseDrainer interface {
	DrainExpiredLeases() []*Lease
}

type ExpiryLoop struct {
	drainer     ExpiredLeaseDrainer
	innerTicker util.Ticker
	propose     util.ProposeFunc
	isLeader    util.IsLeaderFunc
	leadershipC chan bool
	running     atomic.Bool
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *slog.Logger
}

func NewExpiryLoop(logger *slog.Logger, collector ExpiredLeaseDrainer, propose util.ProposeFunc, isLeader util.IsLeaderFunc) *ExpiryLoop {
	return &ExpiryLoop{
		drainer:     collector,
		innerTicker: util.NewRealTicker(expiryLoopInterval),
		propose:     propose,
		isLeader:    isLeader,
		// buffer of one, then use drain-then-send:
		// we dont want to block the observer with stale values
		// so we drain before sending the latest
		leadershipC: make(chan bool, 1),
		logger:      logger.With("component", "expiry_loop"),
	}
}

// drain-then-send, to prevent stale values
func (ex *ExpiryLoop) OnLeadershipGranted() {
	select {
	case <-ex.leadershipC:
	default:
	}
	ex.leadershipC <- true
}

// drain-then-send, to prevent stale values
func (ex *ExpiryLoop) OnLeadershipLost() {
	select {
	case <-ex.leadershipC:
	default:
	}
	ex.leadershipC <- false
}

func (ex *ExpiryLoop) Run(ctx context.Context) {
	if !ex.running.CompareAndSwap(false, true) {
		ex.logger.Warn("Attempted to run expiry loop while it was already running")
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	ex.ctx = ctx
	ex.cancel = cancel
	ex.logger.Info("Expiry loop started started")
	ex.run()
}

func (ex *ExpiryLoop) run() {
	defer ex.innerTicker.Stop()
	var tickerC <-chan time.Time

	if ex.isLeader() {
		tickerC = ex.innerTicker.C()
	}

	for {
		select {
		case granted, ok := <-ex.leadershipC:
			if !ok {
				ex.logger.Error("leadership channel closed, stopping main loop")
				return
			}
			if granted && ex.isLeader() {
				tickerC = ex.innerTicker.C()
			} else {
				tickerC = nil
			}
		case <-ex.ctx.Done():
			ex.logger.Info("context cancelled, stopping main loop", "cause", ex.ctx.Err())
			return
		case <-tickerC:
			ex.tick()
		}
	}
}

func (ex *ExpiryLoop) tick() {
	expired := ex.drainer.DrainExpiredLeases()
	ids := make([]int64, 0, len(expired))
	for _, l := range expired {
		ids = append(ids, l.ID)
	}

	cmd := command.Command{
		Kind: command.KindLeaseExpire,
		LeaseExpired: &command.LeaseExpireCmd{
			ExpiredIDs: ids,
		},
	}
	result, err := ex.propose(ex.ctx, cmd)
	if err != nil {
		ex.logger.Warn(
			"expiry loop error: failed to propose LeaseExpireCmd",
			"error", err,
		)
		return
	}

	if result.Error != nil {
		ex.logger.Warn(
			"expiry loop error: failed to remove expired leases",
			"error", err,
		)
		return
	}
	if result.LeaseExpireResult == nil {
		ex.logger.Warn(
			"expiry loop error: LeaseExpireResult was nil",
		)
		return
	}

	ex.logger.Info("successfuly removed expired leases and their keys",
		"lease_count", result.LeaseExpireResult.RemovedLeaseCount,
		"key_count", result.LeaseExpireResult.RemovedKeyCount,
	)
}

func (ex *ExpiryLoop) Stop() {
	ex.logger.Info("Stopping expiry loop")
	ex.cancel()
	ex.running.Store(false)
}
