package unit

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/fsm"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/store/inmem"
	"github.com/balits/thesis/internal/testx"
	"github.com/balits/thesis/internal/util"
	"github.com/hashicorp/raft"
)

var raftIndex atomic.Uint64 // default 0

type fsmTester struct {
	f *fsm.FSM
	n int
}

func Test_FSM(t *testing.T) {
	t.Run("with_inmemory_storage", func(t *testing.T) {
		tester := fsmTester{fsm.New(inmem.NewStore()), 10}
		t.Run("SET", tester.testSet)
		t.Run("DELETE", tester.testDelete)
		t.Run("BATCH", tester.testBatch)
	})
}

func TestFSM_ApplyThroughRaft(t *testing.T) {
	id := raft.ServerID("dummy")
	_f := fsm.New(inmem.NewStore())

	conf := raft.DefaultConfig()
	loglevel := testx.GetTestingLogLevel()
	conf.Logger = util.NewHcLogAdapter(testx.NewTestLogger(t, loglevel), loglevel)
	conf.LocalID = id
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(""))
	node, err := raft.NewRaft(conf, _f, logs, stable, snaps, trans)
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
		cmd := command.Command{
			Type:  command.CommandTypeSet,
			Key:   []byte("foo"),
			Value: []byte("bar"),
		}
		if err = doRaftApply(node, cmd); err != nil {
			t.Error(err)
		}
	})

	t.Run("FSM Apply Delete through Raft", func(t *testing.T) {
		cmd := command.Command{
			Type: command.CommandTypeDelete,
			Key:  []byte("foo"),
		}
		if err = doRaftApply(node, cmd); err != nil {
			t.Error(err)
		}
	})
}

func doRaftApply(node *raft.Raft, cmd command.Command) error {
	buf := bytes.NewBuffer(make([]byte, 0))
	err := gob.NewEncoder(buf).Encode(&cmd)
	if err != nil {
		return err
	}
	fututre := node.Apply(buf.Bytes(), 2*time.Second)
	if err = fututre.Error(); err != nil {
		return err
	}
	result, ok := fututre.Response().(fsm.AppyResult)
	if !ok {
		return errors.New("Could not cat future.Response() as ApplyResponse")
	}
	return result.Error()
}

func (ft *fsmTester) testSet(t *testing.T) {
	state := make(map[string][]byte, ft.n)
	for i := range ft.n {
		state[fmt.Sprintf("key%d", i)] = DEFAULT_VALUE
	}

	for k, v := range state {
		result, ok := ft.f.Apply(newLog(command.Command{
			Type:  command.CommandTypeSet,
			Key:   []byte(k),
			Value: v,
		})).(fsm.AppyResult)

		if !ok {
			t.Fatal("SET failed:  Apply didnt return fsm.Result")
		}

		if err := result.Error(); err != nil {
			t.Fatal("SET failed:", err)
		}

		stored := result.SetResult

		if stored.ModifyRevision != raftIndex.Load() {
			t.Fatal("SET failed: ModifyRevisions did not match")
		}

		if !bytes.Equal(v, stored.Value) {
			t.Fatal("SET failed: stored values did not match")
		}
	}
}

func (ft *fsmTester) testDelete(t *testing.T) {
	state := make(map[string][]byte, ft.n)
	for i := range ft.n {
		state[fmt.Sprintf("key%d", i)] = DEFAULT_VALUE
	}

	for k, oldValue := range state {
		result, ok := ft.f.Apply(newLog(command.Command{
			Type: command.CommandTypeDelete,
			Key:  []byte(k),
		})).(fsm.AppyResult)

		if !ok {
			t.Fatal("DELETE failed: Apply didnt return fsm.Result")
		}

		if err := result.Error(); err != nil {
			t.Fatal("DELETE failed:", err)
		}

		delete := result.DeleteResult
		if !delete.Deleted {
			t.Fatal("DELETE failed: delete was a no-op, key not found")
		}

		if !bytes.Equal(oldValue, delete.PrevEntry.Value) {
			t.Fatal("DELETE failed: stored values did not match")
		}
	}
}

func (ft *fsmTester) testBatch(t *testing.T) {
	keyGen := func() string {
		return fmt.Sprintf("%b", rand.Intn(5))
	}

	randomState := make(map[string][]byte)
	for i := range ft.n {
		if i%3 == 0 {
			randomState[keyGen()] = b("set")
		} else {
			randomState[keyGen()] = b("delete")
		}
	}

	bc := store.NewBatchCollector()
	for k, v := range randomState {
		if s(v) == "set" {
			bc.RecordSet([]byte(k), v)
		} else if s(v) == "delete" {
			bc.RecordDelete(v)
		}
	}

	normalizedState := make(map[string][]byte)
	maps.Copy(normalizedState, bc.Writes())
	for k, _ := range bc.Deletes() {
		normalizedState[k] = b("")
	}

	batch := make([]command.Command, len(normalizedState))

	for k, v := range randomState {
		cmd := command.Command{Key: []byte(k), Value: v}
		if s(v) == "set" {
			cmd.Type = command.CommandTypeSet
		} else if s(v) == "delete" {
			cmd.Type = command.CommandTypeDelete
		} else {
			continue
		}

		batch = append(batch, cmd)
	}

	result, ok := ft.f.Apply(newLog(command.Command{
		Type:     command.CommandTypeBatch,
		BatchOps: batch,
	})).(fsm.AppyResult)

	if !ok {
		t.Fatal("DELETE failed: Apply didnt return fsm.Result")
	}

	if err := result.Error(); err != nil {
		t.Fatal("BATCH failed:", err)
	}

	if err := result.Error(); err != nil {
		t.Fatal("BATCH failed:", err)
	}

	if !result.BatchResult.Success {
		t.Fatal("BATCH failed: batch result unsuccessful")
	}
}

func newLog(cmd command.Command) *raft.Log {
	bytes, err := command.Encode(cmd)
	if err != nil {
		panic(err)
	}

	logIndex := raftIndex.Add(1)
	return &raft.Log{
		Index: logIndex,
		Data:  bytes,
	}
}
