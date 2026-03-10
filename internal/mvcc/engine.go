package mvcc

import (
	"fmt"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/types"
)

type Engine struct {
	store *KVStore
}

func NewEngine(store *KVStore) *Engine {
	return &Engine{
		store: store,
	}
}

func (e *Engine) ApplyWrite(cmd command.Command) (command.Result, error) {
	switch cmd.Type {
	case command.CmdPut:
		return e.applyPut(cmd.Put)
	case command.CmdDelete:
		return e.applyDelete(cmd.Delete)
	case command.CmdTxn:
		return e.applyTxn(cmd.Txn)
	default:
		return command.Result{}, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (e *Engine) applyPut(cmd *command.PutCmd) (command.Result, error) {
	var prev *types.Entry
	if cmd.PrevEntry {
		prev = e.store.NewReader().Get([]byte(cmd.Key), 0)
	}

	w := e.store.NewWriter()
	rev, err := w.Put([]byte(cmd.Key), []byte(cmd.Value))
	if err != nil {
		w.Abort()
		return command.Result{}, fmt.Errorf("applyPut failed: %w", err)
	}
	w.End()

	return command.Result{
		Header: command.ResultHeader{
			Revision: rev.Main,
		},
		Put: &command.PutResult{
			PrevEntry: prev,
		},
	}, nil
}

func (e *Engine) applyDelete(cmd *command.DeleteCmd) (command.Result, error) {
	var prevs []types.Entry
	if cmd.PrevEntries {
		var err error
		prevs, _, _, err = e.store.NewReader().Range([]byte(cmd.Key), []byte(cmd.End), 0, 0)
		if err != nil {
			return command.Result{}, fmt.Errorf("applyDelete failed: %w", err)
		}
	}
	w := e.store.NewWriter()
	cnt, rev, err := w.DeleteRange([]byte(cmd.Key), []byte(cmd.End))
	if err != nil {
		w.Abort()
		return command.Result{}, fmt.Errorf("applyDelete failed: %w", err)
	}
	w.End()

	return command.Result{
		Header: command.ResultHeader{
			Revision: rev.Main,
		},
		Delete: &command.DeleteResult{
			NumDeleted:  cnt,
			PrevEntries: prevs,
		},
	}, nil
}

func (e *Engine) applyTxn(cmd *command.TxnCmd) (command.Result, error) {
	// Txn has two parts: evaluating conditions (read) and applying txn ops (read/write)
	// since we want this to be atomic, start of by locking the store using a new writer
	w := e.store.NewWriter()

	cond := e.evalCondition(w, cmd.Comparisons)
	var ops []command.TxnOp
	if cond {
		ops = cmd.Success
	} else {
		ops = cmd.Failure
	}

	res, err := e.applyTxnOps(w, ops)
	if err != nil {
		w.Abort()
		return command.Result{}, fmt.Errorf("applyTxn failed: %w", err)
	}
	w.End()

	finalRev, _ := e.store.Revisions()
	return command.Result{
		Header: command.ResultHeader{
			Revision: finalRev.Main,
		},
		Txn: &command.TxnResult{
			Success: cond,
			Results: res,
		},
	}, nil
}

func (e *Engine) evalCondition(w Writer, cmps []command.Comparison) bool {
	if len(cmps) == 0 {
		return true
	}

	for _, cmp := range cmps {
		if !e.evalComparison(w, cmp) {
			return false
		}
	}
	return true
}

func (e *Engine) evalComparison(w Writer, cmp command.Comparison) bool {
	if entry := w.Get([]byte(cmp.Key), 0); entry != nil {
		return cmp.Eval(*entry)
	}
	return cmp.EvalEmpty()
}

// todo: increment revision.sub on every op in the txn, and return the revision of each op in the result
func (e *Engine) applyTxnOps(w Writer, ops []command.TxnOp) ([]command.TxnOpResult, error) {
	res := make([]command.TxnOpResult, 0, len(ops))
	for _, op := range ops {
		switch op.Type {
		case command.TxnOpPut:
			put := op.Put
			var prev *types.Entry
			if put.PrevEntry {
				prev = w.Get([]byte(put.Key), 0)
			}

			_, err := w.Put([]byte(op.Put.Key), []byte(op.Put.Value))
			if err != nil {
				return nil, fmt.Errorf("txn failed: error during put op: %w", err)
			}
			res = append(res, command.TxnOpResult{
				Put: &command.PutResult{
					PrevEntry: prev,
				},
			})
		case command.TxnOpDelete:
			del := op.Delete
			var prevs []types.Entry
			if del.PrevEntries {
				var err error
				prevs, _, _, err = w.Range([]byte(del.Key), []byte(del.End), 0, 0)
				if err != nil {
					return nil, fmt.Errorf("txn failed: error on prev entries on delete op: %w", err)
				}
			}

			cnt, _, err := w.DeleteRange([]byte(del.Key), []byte(del.End))
			if err != nil {
				return nil, fmt.Errorf("txn failed: error during delete op: %w", err)
			}
			res = append(res, command.TxnOpResult{
				Delete: &command.DeleteResult{
					NumDeleted:  cnt,
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
				// TODO: should a read failure terminate the whole txn?
			}
			res = append(res, command.TxnOpResult{
				Range: &command.RangeResult{
					Entries: entries,
					Count:   cnt,
				},
			})
		}
	}

	return res, nil
}
