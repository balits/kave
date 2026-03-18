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
		ex.logger.Warn("Attempted to run expiry loop that was already running")
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
				return
			}
			if granted && ex.isLeader() {
				tickerC = ex.innerTicker.C()
			} else {
				tickerC = nil
			}
		case <-ex.ctx.Done():
			ex.logger.Info("context cancelled, stopping ExpiryLoop",
				"cause", ex.ctx.Err(),
			)
			return
		case <-tickerC:
			ex.tick()
		}
	}
}

func (ex *ExpiryLoop) tick() {
	for _, l := range ex.drainer.DrainExpiredLeases() {
		fut, err := ex.propose(command.Command{
			LeaseRevoke: &command.LeaseRevokeCmd{
				LeaseID: l.ID,
			},
		})
		if err != nil {
			ex.logger.Warn(
				"error proposing LeaseRevokedCommand to the fsm",
				"error", err,
				"lease_id", l.ID,
			)
		}
		if _, err = util.WaitApply(ex.ctx, fut); err != nil {
			ex.logger.Warn(
				"error proposing LeaseRevokedCommand to the fsm",
				"error", err,
				"lease_id", l.ID,
			)
		}
	}
}

func (ex *ExpiryLoop) Stop() {
	ex.logger.Info("Stopping expiry loop")
	ex.cancel()
	ex.running.Store(false)
}
