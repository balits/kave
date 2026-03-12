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

type ExpiredLeaseCollector interface {
	ExpiredLeases() []*Lease
}

type ExpiryLoop struct {
	expiredCollector ExpiredLeaseCollector
	interval         time.Duration
	propose          util.ProposeFunc
	isLeader         util.IsLeaderFunc
	running          atomic.Bool
	ctx              context.Context
	cancel           context.CancelFunc
	logger           *slog.Logger
}

func NewExpiryLoop(logger *slog.Logger, collector ExpiredLeaseCollector, propose util.ProposeFunc, isLeader util.IsLeaderFunc) *ExpiryLoop {
	return &ExpiryLoop{
		expiredCollector: collector,
		interval:         expiryLoopInterval,
		propose:          propose,
		isLeader:         isLeader,
		logger:           logger.With("component", "expiry_loop"),
	}
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
	ticker := time.NewTicker(ex.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ex.ctx.Done():
			ex.logger.Debug("context cancelled")
			return
		case <-ticker.C:
		}

		if !ex.isLeader() {
			// do nothing
			// TODO: maybe use raft.leaderCh in the future (but even that can be faulty)
			continue
		}

		for _, l := range ex.expiredCollector.ExpiredLeases() {
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
}

func (ex *ExpiryLoop) Stop() {
	ex.logger.Info("Stopping expiry loop")
	ex.cancel()
	ex.running.Store(false)
}
