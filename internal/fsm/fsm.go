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
	ErrStateMachineError = errors.New("FSM error")
	ErrNilApplyResult    = fmt.Errorf("%w: nil result from FSM", ErrStateMachineError)
)

type Fsm struct {
	engine *mvcc.Engine

	lm      *lease.LeaseManager
	store   *mvcc.KVStore
	logger  *slog.Logger
	_nodeID string // hack to set Result.Header.NodeID

	metrics *metrics.RaftMetrics
}

func NewFsm(logger *slog.Logger, store *mvcc.KVStore, lm *lease.LeaseManager, nodeID string) *Fsm {
	f := &Fsm{
		engine:  mvcc.NewEngine(store, lm),
		lm:      lm,
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

	var res command.Result

	switch cmd.Type {
	case command.CmdPut, command.CmdDelete, command.CmdTxn:
		res = f.applyKv(cmd)
	case command.CmdLeaseGrant, command.CmdLeaseRevoke, command.CmdLeaseKeepAlive, command.CmdLeaseCheckpoint:
		res = f.applyLease(cmd)
	default:
		panic(fmt.Sprintf("Unsupported command type: %v", cmd.Type))
	}

	res.Header = command.ResultHeader{
		RaftTerm:  log.Term,
		RaftIndex: log.Index,
		NodeID:    f._nodeID,
	}

	return res
}

func (f *Fsm) applyKv(cmd command.Command) command.Result {
	res, err := f.engine.ApplyWrite(cmd)
	if err != nil {
		return command.Result{Error: err}
	}
	return res
}

// TODO: should we create more meaningful return type for lease commands?
func (f *Fsm) applyLease(cmd command.Command) command.Result {
	var (
		res command.Result
		err error
	)

	switch cmd.Type {
	case command.CmdLeaseGrant:
		var lease *lease.Lease
		lease, err = f.lm.Grant(cmd.LeaseGrant.LeaseID, cmd.LeaseGrant.TTL)
		if err == nil {
			res.LeaseGrantResult = &command.LeaseGrantResult{
				TTL:     lease.TTL,
				LeaseID: lease.ID,
			}
		}

	case command.CmdLeaseRevoke:
		var found, revoked bool
		found, revoked = f.lm.Revoke(cmd.LeaseRevoke.LeaseID)
		res.LeaseRevokeResult = &command.LeaseRevokeResult{
			Found:   found,
			Revoked: revoked,
		}

	case command.CmdLeaseKeepAlive:
		var ttl int64
		ttl, err = f.lm.KeepAlive(cmd.LeaseKeepAlive.LeaseID)
		if err == nil {
			res.LeaseKeepAliveResult = &command.LeaseKeepAliveResult{
				TTL:     ttl,
				LeaseID: cmd.LeaseKeepAlive.LeaseID,
			}
		}

	case command.CmdLeaseCheckpoint:
		f.lm.ApplyCheckpoint(*cmd.LeaseCheckpoint)

	case command.CmdLeaseExpired:
		err = f.lm.ApplyExpired(*cmd.LeaseExpired)

	default:
		panic(fmt.Sprintf("Unsupported lease command type: %v", cmd.Type))
	}

	// Lease resultoknál legyen mind az error, mind a result kitölrve
	// igy jóbban látható mi történt
	// TODO: gondoljuk át, talán ez lenne a jobb megoldást a kv parancsoknál is?
	if err != nil {
		res.Error = err
	}
	return res
}

// Snapshot also should be fast, just take a pointer to the data
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.store.Snapshot(), nil
}

// Restore can be slower, it will never run concurrently with Apply.
// Also, no metrics should be replayed during restoration!
func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	err := f.store.Restore(snapshot)
	if err != nil {
		return err
	}
	return f.lm.Restore()
}
