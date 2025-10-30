package api

import (
	"net/http"

	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (s *Server) healthHandler(ctx *web.Context) {
	if s.node.PartOfCluster() && s.node.Raft.State() == raft.Leader || s.node.Raft.State() == raft.Follower {
		ctx.Ok(nil)
	} else {
		ctx.Error("not leader", http.StatusInternalServerError)
	}
	s.node.Raft.Stats()
}
