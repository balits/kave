package unit

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/testx"
	"github.com/balits/thesis/internal/util"
	"github.com/hashicorp/raft"
)

func TestFSM_ApplyInMemoryStore(t *testing.T) {
	testApply(store.NewFSM(store.NewInMemoryStore()), t)
}

func TestFSM_ApplyThroughRaft(t *testing.T) {
	id := raft.ServerID("dummy")
	fsm := store.NewFSM(store.NewInMemoryStore())

	conf := raft.DefaultConfig()
	loglevel := testx.GetTestingLogLevel()
	conf.Logger = util.NewHcLogAdapter(testx.NewTestLogger(t, loglevel), loglevel)
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
		if err = doRaftApply(node, cmd); err != nil {
			t.Error(err)
		}
	})

	t.Run("FSM Apply Delete through Raft", func(t *testing.T) {
		cmd := store.Cmd{
			Kind:  store.CmdKindDelete,
			Key:   "foo",
			Value: []byte(""),
		}
		if err = doRaftApply(node, cmd); err != nil {
			t.Error(err)
		}
	})
}

func testApply(fsm *store.FSM, t *testing.T) {
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

func doApply(fsm *store.FSM, cmd store.Cmd) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return fmt.Errorf("error during encoding: %w", err)
	}

	response := fsm.Apply(newLog(buf.Bytes()))

	applyResponse, ok := response.(store.ApplyResponse)
	if !ok {
		return errors.New("could not cast Apply return type to ApplyResponse")
	}
	if applyResponse.IsError() {
		return applyResponse.GetError()
	}
	return nil
}

func doRaftApply(node *raft.Raft, cmd store.Cmd) error {
	buf := bytes.NewBuffer(make([]byte, 0))
	err := gob.NewEncoder(buf).Encode(&cmd)
	if err != nil {
		return err
	}
	fututre := node.Apply(buf.Bytes(), 2*time.Second)
	if err = fututre.Error(); err != nil {
		return err
	}
	response, ok := fututre.Response().(store.ApplyResponse)
	if !ok {
		return errors.New("Could not cat future.Response() as ApplyResponse")
	}
	if response.IsError() {
		return response.GetError()
	}
	return nil
}

func newLog(data []byte) *raft.Log {
	return &raft.Log{
		Data: data,
	}
}
