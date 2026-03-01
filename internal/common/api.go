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
