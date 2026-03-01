package fsm

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/balits/kave/internal/common"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/storage"
	"github.com/hashicorp/raft"
)

var (
	ErrRevisionMismatch   = errors.New("revisions didn't match")
	ErrUnsupportedCommand = errors.New("unsupported command type")

	// ErrKeyNotFound is returned when a requested key is not found in the store.
	// This is by FSM.Apply to signal a not found error in a result type without
	// it being treated as a much more severe internal storage error
	ErrKeyNotFound = errors.New("key not found")
)

type FSM struct {
	Store      storage.Storage
	fsmMetrics metrics.FsmMetricsAtomic
}

func New(store storage.Storage) *FSM {
	return &FSM{
		Store: store,
	}
}

// ========= raft.FSM impl =========

// Callers should sanitize and validate the commands supplied to the raft.Log,
// to save FSM.Apply computation time. Therefore we assume the log's data is always a well formed command
func (f *FSM) Apply(log *raft.Log) interface{} {
	raftLogIndex := log.Index
	f.fsmMetrics.LastApplyTimeNanos.Store(time.Now().UnixNano())
	f.fsmMetrics.ApplyIndex.Store(raftLogIndex)
	f.fsmMetrics.Term.Store(log.Term)

	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		return AppyResult{err: err}
	}

	switch cmd.Type {
	case CmdSet:
		result, err := f.applySet(raftLogIndex, cmd)
		return AppyResult{
			err:       err,
			SetResult: result,
		}

	case CmdDelete:
		result, err := f.applyDelete(cmd)
		return AppyResult{
			err:          err,
			DeleteResult: result,
		}
	case CmdTxn:
		result, err := f.applyTxn(raftLogIndex, cmd)
		return AppyResult{
			err:       err,
			TxnResult: result,
		}

	default:
		return AppyResult{err: fmt.Errorf("%w: %v", ErrUnsupportedCommand, cmd.Type)}
	}
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.Store.Snapshot()
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	return f.Store.Restore(snapshot)
}

// ======== apply internals ========

func (f *FSM) applySet(currentRevision uint64, cmd Command) (SetResult, error) {
	entity := new(common.Entry)
	rawPrev, err := f.Store.Get(cmd.Bucket, cmd.Key)
	if err != nil {
		// if err == ErrBucketNotFound:
		// 		buckets are not user created, they can only query BucketKV or BucketLease
		// 		if a bucket is not found (which shouldnt happen)
		// 		then its an error
		// if some transaction or disk error happened
		//		thats a valid error
		return nil, err
	}

	if rawPrev == nil {
		entity.Key = cmd.Key
		entity.Value = cmd.Value
		entity.CreateRevision = currentRevision
		entity.ModRevision = currentRevision
		entity.Version = 1
	} else {
		entity, err = common.DecodeEntry(rawPrev)
		if err != nil {
			return nil, err
		}
		entity.Value = cmd.Value
		entity.ModRevision = currentRevision
		entity.Version += 1
	}

	encoded, _ := common.EncodeEntry(entity)
	err = f.Store.Put(cmd.Bucket, cmd.Key, encoded)
	if err != nil {
		return nil, err
	}

	return SetResult(entity), nil
}

func (f *FSM) applyDelete(cmd Command) (*DeleteResult, error) {
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

func (f *FSM) applyTxn(currentRevision uint64, cmd Command) (*TxnResult, error) {
	var (
		kvBucket  = kv.BucketKV // transactions are only supported on the kv bucket, so we can hardcode it here, and skip the bucket field in TxnOp
		txn       = cmd.Txn
		succeeded = false
	)

	for _, cmp := range txn.Compares {
		ok, err := f.evalCompare(kvBucket, cmp)
		if err != nil {
			return nil, err
		}
		if !ok {
			succeeded = false
			break
		}
		succeeded = true
	}

	var ops []TxnOp

	if succeeded {
		ops = txn.Success
	} else {
		ops = txn.Failure
	}

	results, err := f.handleTxnOps(currentRevision, kvBucket, ops)

	if err != nil {
		return nil, err
	}

	return &TxnResult{
		Succeeded: succeeded,
		Results:   results,
	}, nil
}

func (f *FSM) handleTxnOps(currentRevision uint64, bucket storage.Bucket, ops []TxnOp) (results []TxnOpResult, err error) {
	batch, newBatchError := f.Store.NewBatch()
	if newBatchError != nil {
		return nil, newBatchError
	}

	defer func() {
		if newBatchError != nil {
			err = newBatchError
			return
		}

		// some fatal error happened during applying ops,
		// embedd abort error if abort also fails
		if err != nil {
			if err2 := batch.Abort(); err2 != nil {
				err = errors.Join(err2, err)
			}
		}
	}()

	// errors here come in two kinds:
	// 1) errors that should abort the whole transaction, like codec error, or internal storage error
	// 2) errors that that should be recorded as the result of the given operation, like bucket not found
	for _, op := range ops {
		switch op.Type {
		case TxnOpGet:
			var raw []byte
			raw, err = f.Store.Get(bucket, op.Key)
			if err != nil && errors.Is(err, storage.ErrInternalStorageError) {
				return nil, err
			}

			res := new(TxnOpGetResult)
			if raw != nil {
				entry, err := common.DecodeEntry(raw)
				if err != nil {
					return nil, err
				}
				res.Result = entry
			} else {
				if err == nil {
					panic("Store.Get: value and error was both nil")
				}
				res.Err = err
			}

			results = append(results, TxnOpResult{GetResult: res})

		case TxnOpSet:
			rawPrev, err := f.Store.Get(bucket, op.Key)
			if err != nil && errors.Is(err, storage.ErrInternalStorageError) {
				return nil, err
			}

			entity := new(common.Entry)

			if rawPrev == nil {
				entity.Key = op.Key
				entity.Value = op.Value
				entity.CreateRevision = currentRevision
				entity.ModRevision = currentRevision
				entity.Version = 1
			} else {
				entity, err = common.DecodeEntry(rawPrev)
				if err != nil {
					return nil, err
				}

				entity.Value = op.Value
				entity.ModRevision = currentRevision
				entity.Version += 1
			}

			encoded, err := common.EncodeEntry(entity)
			if err != nil {
				return nil, err
			}

			res := new(TxnOpSetResult)

			if err = batch.Put(bucket, op.Key, encoded); err != nil {
				if errors.Is(err, storage.ErrInternalStorageError) {
					return nil, err
				}
				res.Err = err
			} else {
				res.Result = entity
			}

			results = append(results, TxnOpResult{SetResult: res})

		case TxnOpDelete:
			rawPrev, err := f.Store.Get(bucket, op.Key)
			if err != nil && errors.Is(err, storage.ErrInternalStorageError) {
				return nil, err
			}

			res := new(TxnOpDeleteResult)
			if rawPrev != nil {
				prevEntry, err := common.DecodeEntry(rawPrev)
				if err != nil {
					return nil, err
				}

				res.Result = &DeleteResult{
					Deleted:   true,
					PrevEntry: prevEntry,
				}
			}

			if err := batch.Delete(bucket, op.Key); err != nil {
				if errors.Is(err, storage.ErrInternalStorageError) {
					return nil, err
				}
				res.Err = err
				res.Result = nil // if delete failed, then we didnt actually delete anything
			}

			results = append(results, TxnOpResult{
				DeleteResult: res,
			})
		}
	}

	// reset after earlier errors, which couldve been non fatal for the txn
	err = nil
	err = batch.Commit()
	return
}

func (f *FSM) evalCompare(bucket storage.Bucket, cmp Condition) (ok bool, err error) {
	entry := new(common.Entry)
	raw, err := f.Store.Get(bucket, cmp.Key)
	if err != nil {
		return
	}

	if raw == nil {
	} else {
		entry, err = common.DecodeEntry(raw)
		if err != nil {
			return
		}
	}

	// if key is not found, use zero valued entry
	// and let cmp evaluate it as it sees fit
	// for example if they are comparing version to 0 (so key doest exist), then it will pass
	ok = cmp.Eval(entry)
	return
}

func (f *FSM) handleBatch(currentRevision uint64, bucket storage.Bucket, commands []Command) (result AppyResult) {
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

// ========= metrics.FsmMetricsProvider impl =========

func (f *FSM) FsmMetrics() *metrics.FsmMetrics {
	return f.fsmMetrics.FsmMetrics()
}
