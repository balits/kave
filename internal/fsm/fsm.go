package fsm

import (
	"errors"
	"fmt"
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
	engine *mvcc.Engine

	lm      *lease.LeaseManager
	store   *mvcc.KVStore
	logger  *slog.Logger
	_nodeID string // hack to set Result.Header.NodeID

	metrics *metrics.RaftMetrics
}

func NewFsm(logger *slog.Logger, store *mvcc.KVStore, leaseMgr *lease.LeaseManager, nodeID string) *Fsm {
	f := &Fsm{
		engine:  mvcc.NewEngine(store),
		lm:      leaseMgr,
		store:   store,
		logger:  logger.With("component", "fsm"),
		_nodeID: nodeID,
	}
	return f
}

// SetMetrics is needed for a two phase init of the fsm
// because of the unavoidable circular dependency:
//
// 1) raft needs fsm
//
// 2) fsm.metrics need raft
func (f *Fsm) SetMetrics(m *metrics.RaftMetrics) {
	f.metrics = m
}

// Apply should be as fast as possible, therefore:
// 1) validate command structure and arguments before callig Apply
func (f *Fsm) Apply(log *raft.Log) interface{} {
	start := time.Now()
	defer func() { f.metrics.ApplyDurationSec.Observe(time.Since(start).Seconds()) }()
	f.metrics.ApplyTotal.Inc()

	cmd, err := command.Decode(log.Data)
	if err != nil {
		return command.Result{Error: err}
	}

	f.store.UpdateRaftMeta(log.Index, log.Term)

	switch cmd.Type {
	case command.CmdPut, command.CmdDelete, command.CmdTxn:
		return f.applyKv(cmd, log.Term, log.Index)
	case command.CmdLeaseGrant, command.CmdLeaseRevoke, command.CmdLeaseCheckpoint:
		return f.applyLease(cmd)
	default:
		panic(fmt.Sprintf("Unsupported command type: %v", cmd.Type))
	}
}

func (f *Fsm) applyKv(cmd command.Command, term uint64, index uint64) command.Result {
	res, err := f.engine.ApplyWrite(cmd)
	if err != nil {
		return command.Result{Error: err}
	}
	res.Header.RaftTerm = term
	res.Header.RaftIndex = index
	res.Header.NodeID = f._nodeID
	return res
}

// TODO: should we create more meaningful return type for lease commands?
func (f *Fsm) applyLease(cmd command.Command) command.Result {
	switch cmd.Type {
	case command.CmdLeaseCheckpoint:
		f.lm.ApplyCheckpoints(*cmd.LeaseCheckpoint)
	case command.CmdLeaseRevoke:
		f.lm.Revoke(cmd.LeaseRevoke.LeaseID)
	default:
		panic(fmt.Sprintf("Unsupported lease command type: %v", cmd.Type))
	}

	return command.Result{}
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
	return f.lm.Restore(f.store)
}

func (f *Fsm) Metrics() *metrics.RaftMetrics {
	return f.metrics
}
