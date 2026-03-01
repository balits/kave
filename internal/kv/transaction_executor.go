package kv

import (
	"github.com/balits/kave/internal/storage"
)

type TransactionExecutor interface {
	// Execute handles the execution of a command for the given transactional revision
	// Only if the returned error is nil, is the transactional revision safe to merge into the main revision
	// by the revision manager
	Execute(cmd Command, txRev Revision) (*Result, error)

	// ExecutePut handles a inserting or updating a key value pair
	// First it fetches the key metadata from the index
	// If there wasnt any metadata to begin with, we are doing an insert:
	// new metadata is created according to the current revision
	// if there was a metadata, depending if the metadata.tombstone is true or false
	// we update it: if its true, we set CreateRev to the currentRev, and Version = 0
	// else we increment the version
	//
	// this should be an noop, but we should still create a valid delete revision
	// for MVCC's sake, and for the Watches we implement later
	//
	// If we found existing metadata, we fetch the previous entry that could be exist or not
	// then we update the metadata with a new modRevision, set tombstone = true
	// and set a tombstone marker in the history log
	ExecutePut(key, value []byte, rev Revision) (*Result, error)

	// ExecuteDelete handles a keys deletion
	// First it fetches the key metadata from the index
	// If there inst metadata (the key didnt exist before)
	// or the key is already deleted (tombstone in metadata)
	// we return early, no new revision change since no write happened
	//
	// If the value wasnt deleted already, // then we update
	// the metadata with a new modRevision, set tombstone = true
	// and set the value to a tombstone marker in the history log
	ExecuteDelete(key []byte, rev Revision) (*Result, error)

	// ExecuteTxn handles the exection of a transaction command.
	// A transaction commands atomically apply a set of operations depending a set of conditions
	// in the fashion of IF eval(txn.Conditions) THEN apply(txn.SuccessOps) ELSE apply(txn.FailureOps)
	// This results in a {success: bool, result: []Result} response, where success is the result of the conditions evaluation
	// And results is the list of results from the executed ops, be it failure or success ops
	// Like with the other ExecuteXXX methods, we assume the cmd is well formed and valid and we dont vlaidate it here anymore
	// For a TxnCommand that means, for each key there can only be one write operation
	ExecuteTxn(cmd TxnCommand, nextRev Revision) (*TxnResult, error)
}

type executor struct {
	store     storage.Storage
	treeIndex index
}

func NewExecutor(store storage.Storage, treeIndex index) TransactionExecutor {
	return &executor{
		store:     store,
		treeIndex: treeIndex,
	}
}

func (ex *executor) Execute(cmd Command, txRev Revision) (result *Result, err error) {
	switch cmd.Type {
	case CmdPut:
		result, err = ex.ExecutePut(cmd.Key, cmd.Value, txRev)
	case CmdDelete:
		result, err = ex.ExecuteDelete(cmd.Key, txRev)
		return result, nil
	case CmdTxn:
		txres, err := ex.ExecuteTxn(*cmd.Txn, txRev)
		if err == nil && txres != nil {
			result = &Result{TxnResult: txres}
		}
	}

	return
}

func (ex *executor) ExecutePut(key, value []byte, rev Revision) (*Result, error) {
	// update key_history
	newCompositeKey := CompositeKey{
		Key: key,
		Rev: rev,
	}
	err := UpdateKeyHistory(ex.store, newCompositeKey, value)
	if err != nil {
		return nil, err
	}

	// update key_meta metadata
	meta, err := FetchFromKeyMeta(ex.store, key)
	if err != nil {
		return nil, err
	}
	var newMeta *Meta
	if meta != nil {
		// if key was previously deleted, start a "new generation" with new CreateRev and Version
		// otherwise just increment the version
		if meta.Tombstone {
			newMeta = &Meta{
				CreateRev: rev.Main,
				ModRev:    rev.Main,
				Version:   1,
			}
		} else {
			// else keep it as is
			newMeta = &Meta{
				CreateRev: meta.CreateRev,
				ModRev:    rev.Main,
				Version:   meta.Version + 1,
			}
		}
	} else {
		newMeta = &Meta{
			CreateRev: rev.Main,
			ModRev:    rev.Main,
			Version:   1,
		}
	}

	err = UpdateKeyMeta(ex.store, key, *newMeta)
	if err != nil {
		return nil, err
	}

	resultEntry := &Entry{
		Key:   key,
		Value: value,
		Meta:  *newMeta,
	}

	return &Result{SetResult: resultEntry}, nil
}

func (ex *executor) ExecuteDelete(key []byte, rev Revision) (*Result, error) {
	// fetch meta cache, return early if nil
	meta, err := FetchFromKeyMeta(ex.store, key)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return &Result{
			DeleteResult: &DeleteResult{Deleted: false, PrevEntry: nil},
		}, nil
	}

	// already deleted, return early
	if meta.Tombstone {
		return &Result{
			DeleteResult: &DeleteResult{Deleted: false, PrevEntry: nil},
		}, nil
	}

	// if value existed before, fetch prev entry to inclide in DeleteResult.PrevEntry
	prevValue, err := FetchFromKeyHistory(ex.store, CompositeKey{
		Key: key,
		Rev: Revision{
			Main: meta.ModRev,
		},
	})
	if err != nil {
		return nil, err
	}

	// append tombstone to history
	newKey := CompositeKey{
		Key: key,
		Rev: Revision{
			Main: rev.Main,
		},
	}

	if err = UpdateKeyHistory(ex.store, newKey, TombstoneMarker); err != nil {
		return nil, err
	}

	prevMeta := *meta // copy old metadata
	meta.ModRev = rev.Main
	meta.Version++
	meta.Tombstone = true

	// update key_meta
	if err := UpdateKeyMeta(ex.store, key, *meta); err != nil {
		return nil, err
	}

	var prevEntry *Entry
	if prevValue != nil {
		prevEntry = &Entry{
			Key: key,
			Meta: Meta{
				CreateRev: prevMeta.CreateRev,
				ModRev:    prevMeta.ModRev,
				Version:   prevMeta.Version,
				Tombstone: true,
			},
			Value: prevValue,
		}
	}

	result := &Result{
		DeleteResult: &DeleteResult{
			Deleted:   true,
			PrevEntry: prevEntry,
		},
	}

	return result, nil
}

func (ex *executor) ExecuteTxn(cmd TxnCommand, rev Revision) (*TxnResult, error) {
	success, err := ex.evalComparisons(cmd.Comparisons)
	if err != nil {
		return nil, err
	}

	var ops []TxnOp
	if success {
		ops = cmd.Success
	} else {
		ops = cmd.Failure
	}

	if noWriteOps(ops) {
		return &TxnResult{Success: success, Results: nil}, nil
	}

	results, err := ex.applyOps(ops, rev)
	if err != nil {
		return nil, err
	}

	return &TxnResult{Success: success, Results: results}, nil
}

func (ex *executor) evalComparisons(comparisons []Comparison) (bool, error) {
	for _, cmp := range comparisons {
		metaBytes, err := ex.store.Get(BucketKeyMeta, cmp.Key)
		if err != nil {
			return false, err
		}

		// if there is no metadata use nil entry to eval against
		if metaBytes == nil && !cmp.MatchZeroValue() {
			return false, nil
		}

		meta, err := DecodeMeta(metaBytes)
		if err != nil {
			return false, err
		}

		// fetch value only if we need to compare it
		// or return early if its already a tombstone
		var valueBytes []byte
		if cmp.Target == TargetValue {
			if meta.Tombstone && !cmp.MatchZeroValue() {
				return false, nil
			}

			compositeKey := CompositeKey{
				Key: cmp.Key,
				Rev: Revision{
					Main: meta.ModRev,
				},
			}
			compositeKeyBytes, err := EncodeCompositeKey(compositeKey)
			if err != nil {
				return false, err
			}
			valueBytes, err = ex.store.Get(BucketKeyHistory, compositeKeyBytes)
			if err != nil {
				return false, err
			}
			if valueBytes == nil && !cmp.MatchZeroValue() {
				return false, nil
			}
		}

		entry := Entry{
			Key:   cmp.Key,
			Value: valueBytes,
			Meta:  meta,
		}

		if !cmp.Eval(entry) {
			return false, nil
		}
	}

	return true, nil
}

func noWriteOps(ops []TxnOp) bool {
	for _, op := range ops {
		if op.Type == TxnOpPut || op.Type == TxnOpDelete {
			return false
		}
	}
	return true
}

func (ex *executor) applyOps(ops []TxnOp, rev Revision) ([]Result, error) {
	results := make([]Result, 0, len(ops))

	for i, op := range ops {
		var (
			res *Result
			err error
		)

		switch op.Type {
		case TxnOpPut:
			// TODO validate ops
			res, err = ex.ExecutePut(op.Key, op.Value, rev)
		case TxnOpDelete:
			// TODO validate ops
			res, err = ex.ExecuteDelete(op.Key, rev)
		}

		if err != nil {
			return nil, err
		}
		results[i] = *res
		rev.Sub++
	}

	// TODO
	return results, nil
}
