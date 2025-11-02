package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (s *Server) joinHandler(ctx *web.Context) {
	if err := s.node.Raft.VerifyLeader().Error(); err != nil {
		leaderAddress, leaderID := s.node.Raft.LeaderWithID()
		leaderInfo, ok := s.node.Config.GetServiceInfo(string(leaderID))
		if !ok || leaderAddress == "" {
			msg := fmt.Sprintf("No valid leader found, current node was %s", s.node.Raft.State())
			http.Error(ctx.W, msg, http.StatusInternalServerError)
			return
		}

		redirectUrl := fmt.Sprintf("http://%s/join", leaderInfo.GetInternalHttpAddress())
		ctx.W.Header().Set("Location", redirectUrl)
		ctx.W.WriteHeader(http.StatusTemporaryRedirect)
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
			fmt.Fprintln(ctx.W, "node already in the cluster configuration")
			ctx.W.WriteHeader(http.StatusNoContent)
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
