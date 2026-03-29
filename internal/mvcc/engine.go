package mvcc

import (
	"fmt"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/types"
)

type LeaseAttacher interface {
	AttachKey(id int64, key []byte)
	DetachKey(id int64, key []byte)
}

type CommitObserver = func(changes []types.KvEntry)

type Engine struct {
	store    *KvStore
	attacher LeaseAttacher
	obs      []CommitObserver
}

func NewEngine(store *KvStore, attacher LeaseAttacher, obs ...CommitObserver) *Engine {
	return &Engine{
		store:    store,
		attacher: attacher,
		obs:      obs,
	}
}

func (e *Engine) ApplyWrite(cmd command.Command) (*command.Result, error) {
	w := e.store.NewWriter()

	res := new(command.Result)
	var err error

	switch cmd.Kind {
	case command.KindPut:
		var resPut *command.ResultPut
		if resPut, err = e.applyPut(w, cmd.Put); err == nil {
			res.Put = resPut
		}
	case command.KindDelete:
		var resDel *command.ResultDelete
		if resDel, err = e.applyDelete(w, cmd.Delete); err == nil {
			res.Delete = resDel
		}
	case command.KindTxn:
		var resTxn *command.ResultTxn
		if resTxn, err = e.applyTxn(w, cmd.Txn); err == nil {
			res.Txn = resTxn
		}
	default:
		panic(fmt.Sprintf("unknown command type: %s", cmd.Kind))
	}

	if err != nil {
		w.Abort()
		return nil, err
	}

	if err = w.End(); err != nil {
		return nil, err
	}

	_, cs := w.UnsafeExpectedChanges()
	for _, obs := range e.obs {
		obs(cs)
	}
	return res, nil
}

func (e *Engine) applyPut(w Writer, cmd *command.CmdPut) (*command.ResultPut, error) {
	prev := e.store.NewReader().Get([]byte(cmd.Key), 0)

	if prev == nil && (cmd.IgnoreLease || cmd.IgnoreValue) {
		return nil, kv.ErrKeyNotFound
	}

	var (
		key     []byte = cmd.Key
		value   []byte
		leaseID int64
	)

	if cmd.IgnoreValue {
		value = prev.Value
	} else {
		value = cmd.Value
	}

	if cmd.IgnoreLease {
		leaseID = prev.LeaseID
	} else {
		leaseID = cmd.LeaseID
	}

	err := w.Put(key, value, leaseID)
	if err != nil {
		return nil, fmt.Errorf("applyPut failed: %w", err)
	}

	if prev != nil && prev.LeaseID != 0 && prev.LeaseID != leaseID {
		e.attacher.DetachKey(prev.LeaseID, cmd.Key) // no-op if no such lease existed
	}
	if leaseID != 0 {
		e.attacher.AttachKey(cmd.LeaseID, cmd.Key) // no-op if no such lease exists
	}

	res := &command.ResultPut{}
	if cmd.PrevEntry {
		res.PrevEntry = prev
	}
	return res, nil
}

func (e *Engine) applyDelete(w Writer, cmd *command.CmdDelete) (*command.ResultDelete, error) {
	var (
		prevs []types.KvEntry
		err   error
	)
	if cmd.PrevEntries {
		prevs, _, _, err = e.store.NewReader().Range(cmd.Key, cmd.End, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("applyDelete failed: %w", err)
		}
	}

	err = w.DeleteRange(cmd.Key, cmd.End)
	if err != nil {
		return nil, fmt.Errorf("applyDelete failed: %w", err)
	}

	for _, entry := range prevs {
		if entry.LeaseID != 0 {
			e.attacher.DetachKey(entry.LeaseID, entry.Key)
		}
	}

	_, ch := w.UnsafeExpectedChanges()
	return &command.ResultDelete{
		NumDeleted:  int64(len(ch)),
		PrevEntries: prevs,
	}, nil
}

// Txn has two parts: evaluating conditions (read) and applying txn ops (read/write)
func (e *Engine) applyTxn(w Writer, cmd *command.CmdTxn) (*command.ResultTxn, error) {

	cond := e.evalCondition(w, cmd.Comparisons)
	var ops []command.TxnOp
	if cond {
		ops = cmd.Success
	} else {
		ops = cmd.Failure
	}

	res, err := e.applyTxnOps(w, ops)
	if err != nil {
		return nil, fmt.Errorf("applyTxn failed: %w", err)
	}

	return &command.ResultTxn{
		Success: cond,
		Results: res,
	}, nil
}

func (e *Engine) evalCondition(w Writer, cmps []command.Comparison) bool {
	if len(cmps) == 0 {
		return true
	}

	for _, cmp := range cmps {
		entry := w.Get([]byte(cmp.Key), 0)
		if !cmp.Eval(entry) {
			return false
		}
	}
	return true
}

// todo: increment revision.sub on every op in the txn, and return the revision of each op in the result
func (e *Engine) applyTxnOps(w Writer, ops []command.TxnOp) ([]command.TxnOpResult, error) {
	res := make([]command.TxnOpResult, 0, len(ops))
	for _, op := range ops {
		if err := op.Check(); err != nil {
			return nil, fmt.Errorf("invalid operation: %w", err)
		}

		switch op.Type {
		case command.TxnOpPut:
			put := op.Put
			var prev *types.KvEntry
			if put.PrevEntry {
				prev = w.Get(put.Key, 0)
			}

			err := w.Put(put.Key, put.Value, put.LeaseID)
			if err != nil {
				return nil, fmt.Errorf("error during PUT op: %w", err)
			}
			res = append(res, command.TxnOpResult{
				Put: &command.ResultPut{
					PrevEntry: prev,
				},
			})
		case command.TxnOpDelete:
			del := op.Delete
			var prevs []types.KvEntry
			if del.PrevEntries {
				var err error
				prevs, _, _, err = w.Range(del.Key, del.End, 0, 0)
				if err != nil {
					return nil, fmt.Errorf("error during prev entries retrieval on DELETE op: %w", err)
				}
			}

			err := w.DeleteRange(del.Key, del.End)
			if err != nil {
				return nil, fmt.Errorf("error during DELETE op: %w", err)
			}
			_, ch := w.UnsafeExpectedChanges()
			res = append(res, command.TxnOpResult{
				Delete: &command.ResultDelete{
					NumDeleted:  int64(len(ch)),
					PrevEntries: prevs,
				},
			})
		case command.TxnOpRange:
			rng := op.Range
			if rng.Prefix {
				rng.End = kv.PrefixEnd([]byte(rng.Key))
			}
			entries, cnt, _, err := w.Range(rng.Key, rng.End, rng.Revision, rng.Limit)
			if err != nil {
				//should a read failure terminate the whole txn? : YES
				return nil, fmt.Errorf("error during RANGE op: %w", err)
			}
			res = append(res, command.TxnOpResult{
				Range: &command.ResultRange{
					Entries: entries,
					Count:   cnt,
				},
			})
		}
	}

	return res, nil
}
