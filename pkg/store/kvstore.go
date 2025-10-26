// store needs a raft instance
// fsm needs a store instance
// server needs a store instance
package store

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
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
	Value []byte // Value is empty for get and set operations
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
	//
	// Storage operations
	//

	// Mutation -> through raft
	Set(key string, value []byte) error

	// Mutation -> through raft
	Delete(key string) (value []byte, err error)

	// Query -> local read, may be stale (since it doesnt go through raft)
	GetStale(key string) (value []byte, err error)

	//
	// raft.FSM interface methods, that are storage specific used for log replicaion and snapshotting
	//

	// Snapshot returns an FSMSnapshot used to: support log compaction, to // restore the FSM to a previous state, or to bring out-of-date followers up
	// to a recent log index.
	//
	// The Snapshot implementation should return quickly, because Apply can not
	// be called while Snapshot is running. Generally this means Snapshot should
	// only capture a pointer to the state, and any expensive IO should happen
	// as part of FSMSnapshot.Persist.
	//
	// Apply and Snapshot are always called from the same thread, but Apply will
	// be called concurrently with FSMSnapshot.Persist. This means the FSM should
	// be implemented to allow for concurrent updates while a snapshot is happening.
	//
	// Clients of this library should make no assumptions about whether a returned
	// Snapshot() will actually be stored by Raft. In fact it's quite possible that
	// any Snapshot returned by this call will be discarded, and that
	// FSMSnapshot.Persist will never be called. Raft will always call
	// FSMSnapshot.Release however.
	Snapshot() (raft.FSMSnapshot, error)

	// Restore is used to restore an FSM from a snapshot. It is not called
	// concurrently with any other command. The FSM must discard all previous
	// state before restoring the snapshot.
	Restore(snapshot io.ReadCloser) error
}
