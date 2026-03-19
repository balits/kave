package util

import (
	"github.com/hashicorp/raft"
)

type IsLeaderFunc func() bool

func NewIsLeaderFunc(r *raft.Raft, me raft.ServerID) IsLeaderFunc {
	return func() bool {
		_, id := r.LeaderWithID()
		return me == id
	}
}
