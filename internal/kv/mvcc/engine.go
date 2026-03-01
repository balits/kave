package mvcc

import (
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/kv/compaction"
	"github.com/balits/kave/internal/storage"
)

const COMPACTION_WINDOW uint64 = 100

type Engine interface {
	ApplyWrite(cmd kv.Command) (*kv.Result, error)
}

type engine struct {
	store  storage.Storage
	revMgr kv.RevisionManager
	txnEx  kv.TransactionExecutor
	comp   compaction.Compactor
}

func NewEngine(store storage.Storage, revMgr kv.RevisionManager, ex kv.TransactionExecutor, comp compaction.Compactor) Engine {
	return &engine{
		store:  store,
		revMgr: revMgr,
		txnEx:  ex,
		comp:   comp,
	}
}

func (eng *engine) ApplyWrite(cmd kv.Command) (*kv.Result, error) {
	rev := eng.revMgr.BeginTransaction()
	result, err := eng.txnEx.Execute(cmd, rev)
	if err != nil {
		return nil, err
	}

	_, err = eng.revMgr.CommitTransaction(rev)
	if err != nil {
		return nil, err
	}

	return result, nil
}
