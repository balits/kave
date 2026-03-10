package util

import (
	"github.com/balits/kave/internal/config"
	"github.com/hashicorp/raft"
)

type IsLeaderFunc func() bool

func NewIsLeaderFunc(r *raft.Raft, me config.Peer) IsLeaderFunc {
	return func() bool {
		_, id := r.LeaderWithID()
		return me.NodeID == string(id)
	}
}
