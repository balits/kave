package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (s *Server) joinHandler(ctx *web.Context) {
	if s.node.Raft.State() != raft.Leader {
		http.Error(ctx.W, "not leader", http.StatusConflict)
		return
	}

	var body web.JoinBody
	err := ctx.ReadJSON(&body)

	if err != nil {
		http.Error(ctx.W, fmt.Sprintf("error during body parsing: %v", err), http.StatusBadRequest)
		return
	} else if body.ID == "" {
		msg := fmt.Sprintf("%s: missing field 'id'", errMissingOrInvalidFieldsOnRequestBody)
		http.Error(ctx.W, msg, http.StatusBadRequest)
		return
	} else if body.Addr == "" {
		msg := fmt.Sprintf("%s: missing field 'addr'", errMissingOrInvalidFieldsOnRequestBody)
		http.Error(ctx.W, msg, http.StatusBadRequest)
		return
	}

	f := s.node.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		http.Error(ctx.W, fmt.Sprintf("failed to read configuration: %v", err), http.StatusInternalServerError)
		return
	}

	raftConfig := f.Configuration()
	for _, node := range raftConfig.Servers {
		if node.ID == raft.ServerID(body.ID) {
			return
		}
	}

	future := s.node.Raft.AddVoter(raft.ServerID(body.ID), raft.ServerAddress(body.Addr), 0, 5*time.Second)
	if err = future.Error(); err != nil {
		http.Error(ctx.W, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx.W.WriteHeader(http.StatusOK)
}
