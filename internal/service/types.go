package service

import "github.com/balits/kave/internal/common"

type GetRequest struct {
	Key []byte `json:"key"`
}

type GetResponse struct {
	*common.Entry
}

type SetRequest struct {
	Key              []byte  `json:"key"`
	Value            []byte  `json:"value"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
}

type SetResponse struct {
	*common.Entry
}

type DeleteRequest struct {
	Key              []byte  `json:"key"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
}

type DeleteResponse struct {
	Deleted   bool          `json:"deleted"`
	PrevEntry *common.Entry `json:"prev_entry,omitempty"`
}

type TxnRequest struct {
	Ops []TxnOp `json:"ops"`
}

type TxnOpType string

const (
	TxnOpSet    TxnOpType = "set"
	TxnOpDelete TxnOpType = "delete"
)

type TxnOp struct {
	Type  TxnOpType `json:"type"`
	Key   []byte    `json:"key"`
	Value []byte    `json:"value"`
}

type TxnResponse struct {
	Success bool `json:"success"`
}

func txnTypeToCommandType(txnType TxnOpType) common.CommandType {
	switch txnType {
	case TxnOpSet:
		return common.CmdSet
	case TxnOpDelete:
		return common.CmdDelete
	default:
		panic("unknown transaction operation type")
	}
}
