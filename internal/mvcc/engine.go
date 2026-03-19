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

type Engine struct {
	store    *KVStore
	attacher LeaseAttacher
}

func NewEngine(store *KVStore, attacher LeaseAttacher) *Engine {
	return &Engine{
		store:    store,
		attacher: attacher,
	}
}

func (e *Engine) ApplyWrite(cmd command.Command) (command.Result, error) {
	switch cmd.Kind {
	case command.KindPut:
		return e.applyPut(cmd.Put)
	case command.KindDelete:
		return e.applyDelete(cmd.Delete)
	case command.KindTxn:
		return e.applyTxn(cmd.Txn)
	default:
		return command.Result{}, fmt.Errorf("unknown command type: %s", cmd.Kind)
	}
}

func (e *Engine) applyPut(cmd *command.PutCmd) (command.Result, error) {
	prev := e.store.NewReader().Get([]byte(cmd.Key), 0)

	w := e.store.NewWriter()
	rev, err := w.Put(cmd.Key, cmd.Value, cmd.LeaseID)
	if err != nil {
		w.Abort()
		return command.Result{}, fmt.Errorf("applyPut failed: %w", err)
	}
	w.End()

	if prev != nil && prev.LeaseID != 0 && prev.LeaseID != cmd.LeaseID {
		e.attacher.DetachKey(prev.LeaseID, cmd.Key) // no-op if no such lease existed
	}
	if cmd.LeaseID != 0 {
		e.attacher.AttachKey(cmd.LeaseID, cmd.Key) // no-op if no such lease exists
	}

	res := command.Result{
		Header: command.ResultHeader{
			Revision: rev.Main,
		},
		Put: &command.PutResult{},
	}
	if cmd.PrevEntry {
		res.Put.PrevEntry = prev
	}
	return res, nil
}

func (e *Engine) applyDelete(cmd *command.DeleteCmd) (command.Result, error) {
	var (
		prevs []types.KvEntry
		err   error
	)
	if cmd.PrevEntries {
		prevs, _, _, err = e.store.NewReader().Range(cmd.Key, cmd.End, 0, 0)
		if err != nil {
			return command.Result{}, fmt.Errorf("applyDelete failed: %w", err)
		}
	}

	w := e.store.NewWriter()
	cnt, rev, err := w.DeleteRange(cmd.Key, cmd.End)
	if err != nil {
		w.Abort()
		return command.Result{}, fmt.Errorf("applyDelete failed: %w", err)
	}
	w.End()

	for _, entry := range prevs {
		if entry.LeaseID != 0 {
			e.attacher.DetachKey(entry.LeaseID, entry.Key)
		}
	}

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
			var prev *types.KvEntry
			if put.PrevEntry {
				prev = w.Get(put.Key, 0)
			}

			_, err := w.Put(put.Key, put.Value, put.LeaseID)
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
			var prevs []types.KvEntry
			if del.PrevEntries {
				var err error
				prevs, _, _, err = w.Range(del.Key, del.End, 0, 0)
				if err != nil {
					return nil, fmt.Errorf("txn failed: error on prev entries on delete op: %w", err)
				}
			}

			cnt, _, err := w.DeleteRange(del.Key, del.End)
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
