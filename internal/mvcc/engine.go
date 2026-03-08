package mvcc

import (
	"fmt"

	"github.com/balits/kave/internal/kv"
)

// TODO: panic
type Engine struct {
	store *KVStore
}

func NewEngine(store *KVStore) *Engine {
	return &Engine{
		store: store,
	}
}

func (e *Engine) ApplyWrite(cmd kv.Command) (kv.Result, error) {
	switch cmd.Type {
	case kv.CmdPut:
		return e.applyPut(cmd.Put)
	case kv.CmdDelete:
		return e.applyDelete(cmd.Delete)
	case kv.CmdTxn:
		return e.applyTxn(cmd.Txn)
	default:
		return kv.Result{}, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (e *Engine) applyPut(cmd *kv.PutCmd) (kv.Result, error) {
	var prev *kv.Entry
	if cmd.PrevEntry {
		prev = e.store.NewReader().Get([]byte(cmd.Key), 0)
	}

	w := e.store.NewWriter()
	rev, err := w.Put([]byte(cmd.Key), []byte(cmd.Value))
	if err != nil {
		w.Abort()
		return kv.Result{}, fmt.Errorf("applyPut failed: %w", err)
	}
	w.End()

	return kv.Result{
		Header: kv.ResultHeader{
			Revision: rev.Main,
		},
		Put: &kv.PutResult{
			PrevEntry: prev,
		},
	}, nil
}

func (e *Engine) applyDelete(cmd *kv.DeleteCmd) (kv.Result, error) {
	var prevs []kv.Entry
	if cmd.PrevEntries {
		var err error
		prevs, _, _, err = e.store.NewReader().Range([]byte(cmd.Key), []byte(cmd.End), 0, 0)
		if err != nil {
			return kv.Result{}, fmt.Errorf("applyDelete failed: %w", err)
		}
	}
	w := e.store.NewWriter()
	cnt, rev, err := w.DeleteRange([]byte(cmd.Key), []byte(cmd.End))
	if err != nil {
		w.Abort()
		return kv.Result{}, fmt.Errorf("applyDelete failed: %w", err)
	}
	w.End()

	return kv.Result{
		Header: kv.ResultHeader{
			Revision: rev.Main,
		},
		Delete: &kv.DeleteResult{
			NumDeleted:  cnt,
			PrevEntries: prevs,
		},
	}, nil
}

func (e *Engine) applyTxn(cmd *kv.TxnCommand) (kv.Result, error) {
	// Txn has two parts: evaluating conditions (read) and applying txn ops (read/write)
	// since we want this to be atomic, start of by locking the store using a new writer
	w := e.store.NewWriter()

	cond := e.evalCondition(w, cmd.Comparisons)
	var ops []kv.TxnOp
	if cond {
		ops = cmd.Success
	} else {
		ops = cmd.Failure
	}

	res, err := e.applyTxnOps(w, ops)
	if err != nil {
		w.Abort()
		return kv.Result{}, fmt.Errorf("applyTxn failed: %w", err)
	}
	w.End()

	finalRev, _ := e.store.Revisions()
	return kv.Result{
		Header: kv.ResultHeader{
			Revision: finalRev.Main,
		},
		Txn: &kv.TxnResult{
			Success: cond,
			Results: res,
		},
	}, nil
}

func (e *Engine) evalCondition(w Writer, cmps []kv.Comparison) bool {
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

func (e *Engine) evalComparison(w Writer, cmp kv.Comparison) bool {
	if entry := w.Get([]byte(cmp.Key), 0); entry != nil {
		return cmp.Eval(*entry)
	}
	return cmp.Eval(kv.EmptyEntry)
}

// todo: increment revision.sub on every op in the txn, and return the revision of each op in the result
func (e *Engine) applyTxnOps(w Writer, ops []kv.TxnOp) ([]kv.TxnOpResult, error) {
	res := make([]kv.TxnOpResult, 0, len(ops))
	for _, op := range ops {
		switch op.Type {
		case kv.TxnOpPut:
			put := op.Put
			var prev *kv.Entry
			if put.PrevEntry {
				prev = w.Get([]byte(put.Key), 0)
			}

			_, err := w.Put([]byte(op.Put.Key), []byte(op.Put.Value))
			if err != nil {
				return nil, fmt.Errorf("txn failed: error during put op: %w", err)
			}
			res = append(res, kv.TxnOpResult{
				Put: &kv.PutResult{
					PrevEntry: prev,
				},
			})
		case kv.TxnOpDelete:
			del := op.Delete
			var prevs []kv.Entry
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
			res = append(res, kv.TxnOpResult{
				Delete: &kv.DeleteResult{
					NumDeleted:  cnt,
					PrevEntries: prevs,
				},
			})
		case kv.TxnOpRange:
			rng := op.Range
			if rng.Prefix {
				rng.End = kv.PrefixEnd([]byte(rng.Key))
			}
			entries, cnt, _, err := w.Range(rng.Key, rng.End, rng.Revision, rng.Limit)
			if err != nil {
				// TODO: should a read failure terminate the whole txn?
			}
			res = append(res, kv.TxnOpResult{
				Range: &kv.RangeResult{
					Entries: entries,
					Count:   cnt,
				},
			})
		}
	}

	return res, nil
}
