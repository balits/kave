package service

import (
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/types/api"
)

func makeHeader(s mvcc.StoreMetaReader, nodeID string) api.ResponseHeader {
	currRev, compactedRev, raftIndex, raftTerm := s.Meta()
	return api.ResponseHeader{
		Revision:     currRev.Main,
		CompactedRev: compactedRev,
		RaftTerm:     raftTerm,
		RaftIndex:    raftIndex,
		NodeID:       nodeID,
	}
}
