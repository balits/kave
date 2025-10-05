package node

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/balits/thesis/store"
	"github.com/hashicorp/raft"
)

type FSM struct {
	store store.KVStore
}

func NewFSM(store store.KVStore) *FSM {
	return &FSM{store: store}
}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (fsm FSM) Apply(log *raft.Log) interface{} {
	var cmd store.Cmd

	err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&cmd)
	if err != nil {
		panic(err) // fixme: graceful error handling
	}

	fmt.Println("fsm.Apply", cmd)

	switch cmd.Kind {
	case store.CmdKindSet:
		return NewApplyResponse(cmd, fsm.store.Set(cmd.Key, cmd.Value))
	case store.CmdKindDelete:
		value, err := fsm.store.Delete(cmd.Key)
		cmd.Value = value
		return NewApplyResponse(cmd, err)
	case store.CmdKindGet:
		// // we dont care about Cmd.Value in Get requests
		// value, err := fsm.store.GetStale(cmd.Key)
		// r := NewApplyResponse(cmd, err)
		// // hence on success Cmd.Value will contain the value in the store
		// if r.IsError() {
		// 	r.cmd.Value = value
		// }
		// return r
		panic("Get throught raft.Apply not supported")
	}

	return nil
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
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
func (fsm FSM) Snapshot() (raft.FSMSnapshot, error) {
	// fsm.mu.RLock()
	// defer fsm.mu.RUnlock()
	// buffer := bytes.NewBuffer(make([]byte, 0))
	// enc := gob.NewEncoder(buffer)
	// if err := enc.Encode(fsm.hashmap); err != nil {
	// 	return nil, err
	// }
	// snapshot := InMemorySnapshot{bytes: buffer.Bytes()}
	// return snapshot, nil

	return nil, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (fsm FSM) Restore(snapshot io.ReadCloser) error {
	// defer snapshot.Close()
	// dec := gob.NewDecoder(snapshot)
	// var hashmap map[string]string
	// if err := dec.Decode(&hashmap); err != nil {
	// 	return err
	// }
	// fsm.mu.Lock()
	// fsm.hashmap = hashmap
	// fsm.mu.Unlock()
	// return nil

	return nil
}

// ApplyResponse is the concrete type returned by FSM.Apply overwriting the any typed return value in hc's raft, and is accessible by ApplyFuture().Response()
type ApplyResponse struct {
	cmd store.Cmd // fixme: do we even need a reference to the key-value at raft.Apply call site? we would already call it with the cmd
	err error
}

func NewApplyResponse(cmd store.Cmd, err error) ApplyResponse {
	return ApplyResponse{
		cmd: cmd,
		err: err,
	}
}

func (r *ApplyResponse) IsError() bool {
	return r.err != nil
}
