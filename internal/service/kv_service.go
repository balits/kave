package service

import (
	"context"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/common/entry"
)

type KVService interface {
	GetStale(ctx context.Context, req GetRequest) (GetResponse, error)
	GetConsistent(ctx context.Context, req GetRequest) (GetResponse, error)
	Set(ctx context.Context, req SetRequest) (SetResponse, error)
	Delete(ctx context.Context, req DeleteRequest) (DeleteResponse, error)
	Txn(ctx context.Context, req TxnRequest) (TxnResponse, error)
}

type GetRequest struct {
	Key []byte
}

type GetResponse struct {
	*entry.Entry
}

type SetRequest struct {
	Key              []byte
	Value            []byte
	ExpectedRevision *uint64
}

type SetResponse struct {
	*entry.Entry
}

type DeleteRequest struct {
	Key              []byte
	ExpectedRevision *uint64
}

type DeleteResponse struct {
	Deleted   bool
	prevEntry *entry.Entry
}

type TxnRequest struct {
	ops []TxnOp
}

type TxnOpType command.CommandType

const (
	TxnOpSet TxnOpType = iota
	TxnOpDelete
)

type TxnOp struct {
	Type  TxnOpType
	Key   []byte
	Value []byte
}

type TxnResponse struct {
	Success bool
}
