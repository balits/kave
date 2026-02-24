package common

import "errors"

var (
	ErrNotLeader         = errors.New("not leader")
	ErrLeaderNotFound    = errors.New("leader not found")
	ErrStorageError      = errors.New("storage error")
	ErrStateMachineError = errors.New("fsm error")
)
