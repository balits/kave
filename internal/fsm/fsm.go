package fsm

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/metrics"
	"github.com/balits/thesis/internal/store"
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

	cmd, err := command.Decode(log.Data)
	if err != nil {
		return AppyResult{err: err}
	}

	switch cmd.Type {
	case command.CommandTypeGet:
		result, err := f.handleGet(cmd.Key)
		return AppyResult{
			err:       err,
			GetResult: result,
		}

	case command.CommandTypeSet:
		result, err := f.handleSet(raftLogIndex, cmd.Key, cmd.Value, cmd.ExpectedRevision)
		return AppyResult{
			err:       err,
			SetResult: result,
		}

	case command.CommandTypeDelete:
		result, err := f.handleDelete(cmd.Key)
		return AppyResult{
			err:          err,
			DeleteResult: result,
		}

	case command.CommandTypeBatch:
		return f.handleBatch(raftLogIndex, cmd.BatchOps)

	default:
		return AppyResult{err: fmt.Errorf("unsupported command to FSM.Apply: %s", cmd.Type)}
	}
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.Store.Snapshot()
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	return f.Store.Restore(snapshot)
}

// ======== apply internals ========
func (f *FSM) handleGet(key string) (*GetResult, error) {
	raw, err := f.Store.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	return DecodeEntry(raw)
}

func (f *FSM) handleSet(currentRevision uint64, key string, value []byte, expectedRevision *uint64) (*SetResult, error) {
	if expectedRevision == nil {
		return f.set(currentRevision, key, value)
	} else {
		return f.setCAS(currentRevision, *expectedRevision, key, value)
	}
}

func (f *FSM) handleDelete(key string) (*DeleteResult, error) {
	delete := new(DeleteResult)
	rawPrevious, err := f.Store.Delete([]byte(key))
	if err != nil {
		return nil, err
	}

	if rawPrevious != nil {
		prevEntry, err := DecodeEntry(rawPrevious)
		if err != nil {
			return nil, err
		}
		delete.Deleted = true
		delete.PrevEntry = prevEntry
	}

	return delete, nil
}

func (f *FSM) handleBatch(currentRevision uint64, commands []command.Command) (result AppyResult) {
	var batchError error // defer checks this variable, and sets result.err if an error occured

	batch, batchError := f.Store.NewBatch()
	if batchError != nil {
		result.err = batchError
		return
	}

	defer func() {
		// failed to create batch
		if batch == nil {
			result.err = batchError
			return
		}
		// some error happened during batch
		if batchError != nil {
			if err2 := batch.Abort(); err2 != nil {
				result.err = errors.Join(err2, batchError)
			}
			result.err = batchError
			return
		}

		result.BatchResult = &BatchResult{true}
	}()

	for _, cmd := range commands {
		switch cmd.Type {
		case command.CommandTypeSet:
			entry := new(Entry)
			rawPrev, batchError := f.Store.Get([]byte(cmd.Key))
			if batchError != nil {
				return
			}

			if rawPrev == nil {
				entry.Key = []byte(cmd.Key)
				entry.Value = cmd.Value
				entry.CreateRevision = currentRevision
				entry.ModifyRevision = currentRevision
				entry.Version = 1
			} else {
				entry, _ = DecodeEntry(rawPrev)
				entry.Value = cmd.Value
				entry.ModifyRevision = currentRevision
				entry.Version += 1
			}
			encoded, batchError := EncodeEntry(entry)
			if batchError != nil {
				return
			}

			batchError = batch.Set([]byte(cmd.Key), encoded)
			if batchError != nil {
				return
			}
		case command.CommandTypeDelete:
			batchError = batch.Delete([]byte(cmd.Key))
			if batchError != nil {
				return
			}
			// case CommandTypeCompareAndSwap:
			// 	panic("Batch CAS unimplemented")
		}
	}

	batchError = batch.Commit()
	return
}

func (f *FSM) set(currentRevision uint64, key string, value []byte) (*SetResult, error) {
	entry := new(Entry)
	rawPrev, err := f.Store.Get([]byte(key))
	if rawPrev == nil {
		entry.Key = []byte(key)
		entry.Value = value
		entry.CreateRevision = currentRevision
		entry.ModifyRevision = currentRevision
		entry.Version = 1
	} else {
		entry, _ = DecodeEntry(rawPrev)
		entry.Value = value
		entry.ModifyRevision = currentRevision
		entry.Version += 1
	}

	encoded, _ := EncodeEntry(entry)
	err = f.Store.Set([]byte(key), encoded)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (f *FSM) setCAS(currentRevision, expectedRevision uint64, key string, value []byte) (*SetResult, error) {
	newEntry := new(Entry)
	rawPrev, err := f.Store.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	// if no prev value was present, we either insert a new one if expectedRevision is 0
	// otherwise we cant compare to anything -> error
	if rawPrev == nil {
		if expectedRevision != 0 {
			return nil, errors.New("no previous value to compare revisions with")
		}
		newEntry = &Entry{
			Key:            []byte(key),
			Value:          value,
			CreateRevision: currentRevision,
			ModifyRevision: currentRevision,
			Version:        1,
		}
	} else {
		prevEntry, err := DecodeEntry(rawPrev)
		if err != nil {
			return nil, err
		}

		if prevEntry.ModifyRevision != expectedRevision {
			return nil, ErrRevisionMismatch
		}

		newEntry = &Entry{
			Key:            prevEntry.Key,
			Value:          value,
			CreateRevision: prevEntry.CreateRevision,
			ModifyRevision: currentRevision,
			Version:        prevEntry.Version + 1,
		}
	}

	newBytes, err := EncodeEntry(newEntry)
	if err != nil {
		return nil, err
	}

	err = f.Store.Set([]byte(key), newBytes)
	if err != nil {
		return nil, err
	}

	return newEntry, nil
}

// ========= metrics.FsmMetricsProvider impl =========

func (f *FSM) FsmMetrics() *metrics.FsmMetrics {
	return f.fsmMetrics.FsmMetrics()
}
