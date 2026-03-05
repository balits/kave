package fsm

import (
	"errors"
	"io"
	"log/slog"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/hashicorp/raft"
)

var (
	ErrStateMachineError = errors.New("state machine error")
)

type Fsm struct {
	engine  *mvcc.Engine
	store   *mvcc.KVStore
	logger  *slog.Logger
	_nodeID string // hack to set Result.Header.NodeID
}

func NewFsm(logger *slog.Logger, store *mvcc.KVStore, nodeID string) *Fsm {
	return &Fsm{
		engine:  mvcc.NewEngine(store),
		store:   store,
		logger:  logger.With("component", "fsm"),
		_nodeID: nodeID,
	}
}

// Apply should be as fast as possible, therefore:
// 1) validate command structure and arguments before callig Apply
func (f *Fsm) Apply(log *raft.Log) interface{} {
	cmd, err := kv.DecodeCommand(log.Data)
	if err != nil {
		return kv.Result{Error: err}
	}

	f.store.UpdateRaftMeta(log.Index, log.Term)

	res, err := f.engine.ApplyWrite(cmd)
	if err != nil {
		return kv.Result{Error: err}
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
	return f.store.Restore(snapshot)
}
