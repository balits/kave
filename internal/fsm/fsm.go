package fsm

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/balits/kave/internal/common"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/store"
	"github.com/hashicorp/raft"
)

var (
	ErrRevisionMismatch = errors.New("revisions didn't match")
)

type FSM struct {
	Store      store.Storage
	fsmMetrics metrics.FsmMetricsAtomic
}

func New(store store.Storage) *FSM {
	return &FSM{
		Store: store,
	}
}

// ========= raft.FSM impl =========

func (f *FSM) Apply(log *raft.Log) interface{} {
	raftLogIndex := log.Index
	f.fsmMetrics.LastApplyTimeNanos.Store(time.Now().UnixNano())
	f.fsmMetrics.ApplyIndex.Store(raftLogIndex)
	f.fsmMetrics.Term.Store(log.Term)

	cmd, err := common.DecodeCommand(log.Data)
	if err != nil {
		return AppyResult{err: err}
	}

	switch cmd.Type {
	case common.CmdSet:
		result, err := f.handleSet(raftLogIndex, cmd)
		return AppyResult{
			err:       err,
			SetResult: result,
		}

	case common.CmdDelete:
		result, err := f.handleDelete(cmd)
		return AppyResult{
			err:          err,
			DeleteResult: result,
		}

	default:
		return AppyResult{err: fmt.Errorf("unsupported types.to FSM.Apply: %s", cmd.Type)}
	}
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.Store.Snapshot()
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	return f.Store.Restore(snapshot)
}

// ======== apply internals ========

func (f *FSM) handleSet(currentRevision uint64, cmd common.Command) (SetResult, error) {
	if cmd.ExpectedRevision == nil {
		return f.set(currentRevision, cmd.Bucket, cmd.Key, cmd.Value)
	} else {
		return f.setCAS(currentRevision, *cmd.ExpectedRevision, cmd.Bucket, cmd.Key, cmd.Value)
	}
}

func (f *FSM) handleDelete(cmd common.Command) (*DeleteResult, error) {
	result := new(DeleteResult)
	rawPrevious, err := f.Store.Delete(cmd.Bucket, cmd.Key)
	if err != nil {
		return nil, err
	}

	if rawPrevious != nil {
		prevEntry, err := common.DecodeEntry(rawPrevious)
		if err != nil {
			return nil, err
		}
		result.Deleted = true
		result.PrevEntry = prevEntry
	}

	return result, nil
}

func (f *FSM) handleBatch(currentRevision uint64, bucket store.Bucket, commands []common.Command) (result AppyResult) {
	// var batchError error // defer checks this variable, and sets result.err if an error occured

	// batch, batchError := f.Store.NewBatch()
	// if batchError != nil {
	// 	result.err = batchError
	// 	return
	// }

	// defer func() {
	// 	// failed to create batch
	// 	if batch == nil {
	// 		result.err = batchError
	// 		return
	// 	}
	// 	// some error happened during batch
	// 	if batchError != nil {
	// 		if err2 := batch.Abort(); err2 != nil {
	// 			result.err = errors.Join(err2, batchError)
	// 		}
	// 		result.err = batchError
	// 		return
	// 	}

	// 	result.BatchResult = &BatchResult{true}
	// }()

	// for _, cmd := range commands {
	// 	switch cmd.Type {
	// 	case common.CmdSet:
	// 		ent := new(common.Entry)
	// 		rawPrev, batchError := f.Store.Get([]byte(cmd.Key))
	// 		if batchError != nil {
	// 			return
	// 		}

	// 		if rawPrev == nil {
	// 			ent.Key = []byte(cmd.Key)
	// 			ent.Value = cmd.Value
	// 			ent.CreateRevision = currentRevision
	// 			ent.ModifyRevision = currentRevision
	// 			ent.Version = 1
	// 		} else {
	// 			ent, _ = common.DecodeEntry(rawPrev)
	// 			ent.Value = cmd.Value
	// 			ent.ModifyRevision = currentRevision
	// 			ent.Version += 1
	// 		}
	// 		encoded, batchError := common.EncodeEntry(ent)
	// 		if batchError != nil {
	// 			return
	// 		}

	// 		batchError = batch.Set([]byte(cmd.Key), encoded)
	// 		if batchError != nil {
	// 			return
	// 		}
	// 	case common.CmdDelete:
	// 		batchError = batch.Delete([]byte(cmd.Key))
	// 		if batchError != nil {
	// 			return
	// 		}
	// 		// case types.ypeCompareAndSwap:
	// 		// 	panic("Batch CAS unimplemented")
	// 	}
	// }

	// batchError = batch.Commit()
	// return
	return AppyResult{BatchResult: &BatchResult{false}}
}

func (f *FSM) set(currentRevision uint64, bucket store.Bucket, key []byte, value []byte) (SetResult, error) {
	ent := new(common.Entry)
	rawPrev, err := f.Store.Get(bucket, key)
	if rawPrev == nil {
		ent.Key = key
		ent.Value = value
		ent.CreateRevision = currentRevision
		ent.ModifyRevision = currentRevision
		ent.Version = 1
	} else {
		ent, _ = common.DecodeEntry(rawPrev)
		ent.Value = value
		ent.ModifyRevision = currentRevision
		ent.Version += 1
	}

	encoded, _ := common.EncodeEntry(ent)
	err = f.Store.Set(bucket, key, encoded)
	if err != nil {
		return SetResult{}, err
	}

	return SetResult{ent}, nil
}

func (f *FSM) setCAS(currentRevision, expectedRevision uint64, bucket store.Bucket, key []byte, value []byte) (SetResult, error) {
	newEntry := new(common.Entry)
	rawPrev, err := f.Store.Get(bucket, key)
	if err != nil {
		return SetResult{}, err
	}

	// if no prev value was present, we either insert a new one if expectedRevision is 0
	// otherwise we cant compare to anything -> error
	if rawPrev == nil {
		if expectedRevision != 0 {
			return SetResult{}, errors.New("no previous value to compare revisions with")
		}

		newEntry = &common.Entry{
			Key:            key,
			Value:          value,
			CreateRevision: currentRevision,
			ModifyRevision: currentRevision,
			Version:        1,
		}
	} else {
		prevEntry, err := common.DecodeEntry(rawPrev)
		if err != nil {
			return SetResult{}, err
		}

		if prevEntry.ModifyRevision != expectedRevision {
			return SetResult{}, ErrRevisionMismatch
		}

		newEntry = &common.Entry{
			Key:            prevEntry.Key,
			Value:          value,
			CreateRevision: prevEntry.CreateRevision,
			ModifyRevision: currentRevision,
			Version:        prevEntry.Version + 1,
		}
	}

	newBytes, err := common.EncodeEntry(newEntry)
	if err != nil {
		return SetResult{}, err
	}

	err = f.Store.Set(bucket, key, newBytes)
	if err != nil {
		return SetResult{}, err
	}

	return SetResult{newEntry}, nil
}

// ========= metrics.FsmMetricsProvider impl =========

func (f *FSM) FsmMetrics() *metrics.FsmMetrics {
	return f.fsmMetrics.FsmMetrics()
}
