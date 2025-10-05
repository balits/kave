// store needs a raft instance
// fsm needs a store instance
// server needs a store instance
package store

import (
	"fmt"
)

type CmdKind string

const (
	CmdKindGet    CmdKind = "get"
	CmdKindSet    CmdKind = "set"
	CmdKindDelete CmdKind = "del"
)

type Cmd struct {
	Kind  CmdKind
	Key   string
	Value string // Value is "" for get and set operations
}

var (
	ErrorNotLeader   = fmt.Errorf("not leader")
	ErrorKeyNotFound = fmt.Errorf("key not found")
)

// KVStore is the raft backed key-value storage interface that
// needs to be implemented by all storage services (ephemeral or durable).
// It serves two purpose, first are the common key-value operations like get, set or delete.
// Second, each KVStore implementation should also implement raft.FSM,
// fuesing the two interfaces together.
type KVStore interface {
	// Mutation -> through raft
	Set(key, value string) error

	// Mutation -> through raft
	Delete(key string) (string, error)

	// Query -> local read, may be stale (since it doesnt go through raft)
	GetStale(key string) (string, error)
}
