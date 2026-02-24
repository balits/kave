package common

import "github.com/hashicorp/raft"

type GetRequest struct {
	Key []byte `json:"key"`
}

type GetResponse struct {
	*Entry
}

type SetRequest struct {
	Key              []byte  `json:"key"`
	Value            []byte  `json:"value"`
	ExpectedRevision *uint64 `json:"revision,omitempty"`
}

type SetResponse struct {
	*Entry
}

type DeleteRequest struct {
	Key              []byte  `json:"key"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
}

type DeleteResponse struct {
	Deleted   bool   `json:"deleted"`
	PrevEntry *Entry `json:"prev_entry,omitempty"`
}

type JoinRequest struct {
	NodeID   raft.ServerID      `json:"node_id"`
	RaftAddr raft.ServerAddress `json:"raftaddr"`
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

func TxnTypeToCommandType(txnType TxnOpType) CommandType {
	switch txnType {
	case TxnOpSet:
		return CmdSet
	case TxnOpDelete:
		return CmdDelete
	default:
		panic("unknown transaction operation type")
	}
}
