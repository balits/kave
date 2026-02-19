package common

import "errors"

var (
	ErrNotLeader         = errors.New("not leader")
	ErrKeyNotFound       = errors.New("not leader")
	ErrStorageError      = errors.New("not leader")
	ErrStateMachineError = errors.New("not leader")
)
