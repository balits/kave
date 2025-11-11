package store

import "github.com/hashicorp/raft"

type FSMStore interface {
	raft.FSM
	KVStore
}

// ApplyResponse is the concrete type returned by FSM.Apply overwriting the any typed return value in hc's raft, and is accessible by ApplyFuture().Response()
type ApplyResponse struct {
	Cmd Cmd // question: do we even need a reference to the key-value at raft.Apply call site? we would already call it with the cmd. answer: we use cmd in delete for to return the deleted value
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
