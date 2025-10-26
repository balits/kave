package service

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/balits/thesis/pkg/store"
	"github.com/hashicorp/raft"
)

func TestFSM_ApplyInMemoryStore(t *testing.T) {
	testApply(store.NewInMemoryStore(), t)
}

func TestFSM_ApplyThroughRaft(t *testing.T) {
	id := raft.ServerID("dummy")
	fsm := NewFSM(store.NewInMemoryStore())

	conf := raft.DefaultConfig()
	conf.LocalID = id
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(""))
	node, err := raft.NewRaft(conf, fsm, logs, stable, snaps, trans)
	if err != nil {
		t.Errorf("could not create raft node: %v", err)
	}

	f := node.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      id,
				Address: trans.LocalAddr(),
			},
		},
	})

	if err = f.Error(); err != nil {
		t.Errorf("could not bootstrap raft node: %v", err)
	}
	// time.Sleep(2 * time.Second)
	<-node.LeaderCh() // wait until node becomes leader (pre voting, voting election etc)

	t.Run("FSM Apply Set through Raft", func(t *testing.T) {
		cmd := store.Cmd{
			Kind:  store.CmdKindSet,
			Key:   "foo",
			Value: []byte("bar"),
		}
		if err := doApply(node, cmd); err != nil {
			t.Error(err)
		}
	})

	t.Run("FSM Apply Delete through Raft", func(t *testing.T) {
		cmd := store.Cmd{
			Kind:  store.CmdKindDelete,
			Key:   "foo",
			Value: []byte(""),
		}
		if err := doApply(node, cmd); err != nil {
			t.Error(err)
		}
	})
}

func testApply(s store.KVStore, t *testing.T) {
	fsm := NewFSM(s)
	t.Run("FSM Apply Set", func(t *testing.T) {
		cmd := store.Cmd{
			Kind:  store.CmdKindSet,
			Key:   "foo",
			Value: []byte("bar"),
		}
		if err := doApply(fsm, cmd); err != nil {
			t.Error(err)
		}
	})

	t.Run("FSM Apply Delete", func(t *testing.T) {
		cmd := store.Cmd{
			Kind:  store.CmdKindDelete,
			Key:   "foo",
			Value: []byte(""),
		}
		if err := doApply(fsm, cmd); err != nil {
			t.Error(err)
		}
	})
}

func doApply(applier any, cmd store.Cmd) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return fmt.Errorf("error during encoding: %w", err)
	}

	var response any
	switch a := applier.(type) {
	case *FSM:
		response = a.Apply(newLog(buf.Bytes()))
	case *raft.Raft:
		future := a.Apply(buf.Bytes(), time.Second*5)
		if err := future.Error(); err != nil {
			return err
		}
		response = future.Response()
	default:
		return errors.New("applier was an unknown type")
	}

	applyResponse, ok := response.(ApplyResponse)
	if !ok {
		return errors.New("could not cast Apply return type to ApplyResponse")
	}
	if applyResponse.IsError() {
		return applyResponse.err
	}
	return nil
}

func newLog(data []byte) *raft.Log {
	return &raft.Log{
		Data: data,
	}
}
