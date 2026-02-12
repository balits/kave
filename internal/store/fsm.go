package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"time"

	"github.com/balits/thesis/internal/metrics"
	"github.com/hashicorp/raft"
)

type FSM struct {
	Store      Storage
	fsmMetrics metrics.FsmMetricsAtomic
}

func NewFSM(store Storage) *FSM {
	return &FSM{
		Store: store,
	}
}

// ========= raft.FSM impl =========

func (f *FSM) Apply(log *raft.Log) interface{} {
	f.fsmMetrics.LastApplyTimeNanos.Store(time.Now().UnixNano())
	f.fsmMetrics.ApplyIndex.Store(log.Index)
	f.fsmMetrics.Term.Store(log.Term)

	var cmd Cmd

	err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&cmd)
	if err != nil {
		return NewApplyResponse(cmd, err)
	}

	switch cmd.Kind {
	case CmdKindSet:
		_, err := f.Store.Set(cmd.Key, cmd.Value)
		return NewApplyResponse(cmd, err)
	case CmdKindDelete:
		value, err := f.Store.Delete(cmd.Key)
		cmd.Value = value
		return NewApplyResponse(cmd, err)
	case CmdKindGet:
		// // we dont care about Cmd.Value in Get requests
		// value, err := fsm.store.GetStale(cmd.Key)
		// r := NewApplyResponse(cmd, err)
		// // hence on success Cmd.Value will contain the value in the store
		// if r.IsError() {
		// 	r.cmd.Value = value
		// }
		// return r
		return NewApplyResponse(cmd, errors.New("get commands throught raft are not supported"))
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.Store.Snapshot()
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	return f.Store.Restore(snapshot)
}

// ========= metrics.FsmMetricsProvider impl =========
func (f *FSM) FsmMetrics() *metrics.FsmMetrics {
	return f.fsmMetrics.FsmMetrics()
}

// ApplyResponse is the concrete type returned by FSM.Apply overwriting the any typed return value in hc's raft, and is accessible by ApplyFuture().Response()
type ApplyResponse struct {
	Cmd Cmd
	err error
}

func NewApplyResponse(cmd Cmd, err error) ApplyResponse {
	return ApplyResponse{
		Cmd: cmd,
		err: err,
	}
}

func (r ApplyResponse) GetError() error {
	return r.err
}

func (r ApplyResponse) IsError() bool {
	return r.err != nil
}
