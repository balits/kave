package service

import (
	"github.com/balits/kave/internal/common"
)

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
