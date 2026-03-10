package fsm

import (
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/hashicorp/raft"
)

var (
	ErrStateMachineError = errors.New("state machine error")
)

type Fsm struct {
	engine   *mvcc.Engine
	leaseMgr *lease.LeaseManager
	store    *mvcc.KVStore
	logger   *slog.Logger
	_nodeID  string // hack to set Result.Header.NodeID

	metrics *metrics.RaftMetrics
}

func NewFsm(logger *slog.Logger, store *mvcc.KVStore, leaseMgr *lease.LeaseManager, nodeID string) *Fsm {
	f := &Fsm{
		engine:   mvcc.NewEngine(store),
		leaseMgr: leaseMgr,
		store:    store,
		logger:   logger.With("component", "fsm"),
		_nodeID:  nodeID,
	}
	return f
}

func (f *Fsm) InjectMetrics(m *metrics.RaftMetrics) {
	f.metrics = m
}

// Apply should be as fast as possible, therefore:
// 1) validate command structure and arguments before callig Apply
func (f *Fsm) Apply(log *raft.Log) interface{} {
	start := time.Now()
	defer f.metrics.ApplyDurationSec.Observe(time.Since(start).Seconds())
	f.metrics.ApplyTotal.Inc()

	cmd, err := command.Decode(log.Data)
	if err != nil {
		return command.Result{Error: err}
	}

	f.store.UpdateRaftMeta(log.Index, log.Term)

	res, err := f.engine.ApplyWrite(cmd)
	if err != nil {
		return command.Result{Error: err}
	}
	res.Header.RaftTerm = log.Term
	res.Header.RaftIndex = log.Index
	res.Header.NodeID = f._nodeID

	return res
}

// Snapshot also should be fast, just take a pointer to the data
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.store.Snapshot(), nil
}

// Restore can be slower, it will never run concurrently with Apply
func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	err := f.store.Restore(snapshot)
	if err != nil {
		return err
	}
	return f.leaseMgr.Restore(f.store)
}

func (f *Fsm) Metrics() *metrics.RaftMetrics {
	return f.metrics
}
