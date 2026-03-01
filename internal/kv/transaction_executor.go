package kv

import (
	"fmt"

	"github.com/balits/kave/internal/backend"
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
	bck       backend.Backend
	treeIndex index
}

func NewExecutor(b backend.Backend, treeIndex index) TransactionExecutor {
	return &executor{
		bck:       b,
		treeIndex: treeIndex,
	}
}

func (ex *executor) Execute(cmd Command, txRev Revision) (result *Result, err error) {
	switch cmd.Type {
	case CmdPut:
		result, err = ex.ExecutePut(cmd.Key, cmd.Value, txRev)
	case CmdDelete:
		result, err = ex.ExecuteDelete(cmd.Key, txRev)
	case CmdTxn:
		txres, err := ex.ExecuteTxn(*cmd.Txn, txRev)
		if err == nil && txres != nil {
			result = &Result{TxnResult: txres}
		}
	}

	return
}

func (ex *executor) ExecutePut(key, value []byte, rev Revision) (*Result, error) {
}

func (ex *executor) ExecuteDelete(key []byte, rev Revision) (*Result, error) {
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

	for _, op := range ops {
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
		results = append(results, *res)
		rev.Sub++
	}

	// TODO
	return results, nil
}
