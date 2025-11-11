package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

type InMemoryStore struct {
	sync.RWMutex
	hashmap map[string][]byte
}

// NewInMemoryStore creates a new, empty in-memory key-value store
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{sync.RWMutex{}, make(map[string][]byte)}
}

// ========= store.KVStore impl =========

func (s *InMemoryStore) GetStale(key string) (value []byte, err error) {
	s.RLock()
	defer s.RUnlock()
	value, ok := s.hashmap[key]
	if !ok {
		err = ErrorKeyNotFound
	}
	return
}

func (s *InMemoryStore) Set(key string, value []byte) error {
	s.Lock()
	defer s.Unlock()
	s.hashmap[key] = value
	return nil
}

func (s *InMemoryStore) Delete(key string) (value []byte, err error) {
	s.Lock()
	defer s.Unlock()
	value, ok := s.hashmap[key]
	if !ok {
		err = ErrorKeyNotFound // question: maybe move to noop on key not found
	}
	delete(s.hashmap, key)
	return
}

// ========= raft.FSM impl =========

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (s *InMemoryStore) Apply(log *raft.Log) interface{} {
	var cmd Cmd

	err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&cmd)
	if err != nil {
		return NewApplyResponse(cmd, err)
	}

	switch cmd.Kind {
	case CmdKindSet:
		return NewApplyResponse(cmd, s.Set(cmd.Key, cmd.Value))
	case CmdKindDelete:
		value, err := s.Delete(cmd.Key)
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
func (s *InMemoryStore) Snapshot() (raft.FSMSnapshot, error) {
	s.RLock()
	defer s.RUnlock()
	data := maps.Clone(s.hashmap)
	return InMemorySnapshot{data: data}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (s *InMemoryStore) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	var newmap map[string][]byte
	err := gob.NewDecoder(snapshot).Decode(&newmap)
	if err != nil {
		return err
	}
	s.Lock()
	s.hashmap = newmap
	s.Unlock()
	return nil
}

// InMemorySnapshot is a snapshot of the data at a given time, used to support log compaction and restore the FSM to a desired state.
// It is returned FSM.Snapshot() which itself shouldn't do heavy IO work.
// No mutex needed since snapshots are immutable
type InMemorySnapshot struct {
	data map[string][]byte
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s InMemorySnapshot) Persist(sink raft.SnapshotSink) error {
	err := gob.NewEncoder(sink).Encode(s.data)
	if err != nil {
		err2 := sink.Cancel()
		if err != nil {
			return errors.Join(err, err2)
		}
		return err
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (s InMemorySnapshot) Release() {
	//let gc clean it up later automatically
}
