package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (s *Server) joinHandler(ctx *web.Context) {
	if s.node == nil || s.node.Raft == nil {
		status := http.StatusInternalServerError
		ctx.Status = status
		http.Error(ctx.W, "Raft unavailable", status)
		return
	}

	if err := s.node.Raft.VerifyLeader().Error(); err != nil {
		leaderAddress, leaderID := s.node.Raft.LeaderWithID()
		leader, ok := s.node.Config.FindPeer(string(leaderID))
		if !ok || leaderAddress == "" {
			status := http.StatusServiceUnavailable
			ctx.Status = status
			msg := fmt.Sprintf("No valid leader found, current node was %s", s.node.Raft.State())
			http.Error(ctx.W, msg, status)
			return
		}

		status := http.StatusTemporaryRedirect
		ctx.Status = status
		redirectUrl := fmt.Sprintf("http://%s/join", leader.GetInternalHttpAddress())
		ctx.W.Header().Set("Location", redirectUrl)
		ctx.W.WriteHeader(status)
		return
	}

	var body web.JoinBody
	err := ctx.ReadJSON(&body)

	if err != nil {
		status := http.StatusBadRequest
		ctx.Status = status
		http.Error(ctx.W, fmt.Sprintf("error during body parsing: %v", err), status)
		return
	} else if body.ID == "" {
		status := http.StatusBadRequest
		ctx.Status = status
		msg := fmt.Sprintf("%s: missing field 'id'", errMissingOrInvalidFieldsOnRequestBody)
		http.Error(ctx.W, msg, status)
		return
	} else if body.Addr == "" {
		status := http.StatusBadRequest
		ctx.Status = status
		msg := fmt.Sprintf("%s: missing field 'addr'", errMissingOrInvalidFieldsOnRequestBody)
		http.Error(ctx.W, msg, status)
		return
	}

	f := s.node.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		status := http.StatusInternalServerError
		ctx.Status = status
		http.Error(ctx.W, fmt.Sprintf("failed to read configuration: %v", err), status)
		return
	}

	raftConfig := f.Configuration()
	for _, node := range raftConfig.Servers {
		if node.ID == raft.ServerID(body.ID) {
			status := http.StatusNoContent
			ctx.Status = status
			ctx.W.WriteHeader(status)
			return
		}
	}

	future := s.node.Raft.AddVoter(raft.ServerID(body.ID), raft.ServerAddress(body.Addr), 0, 5*time.Second)
	if err = future.Error(); err != nil {
		status := http.StatusInternalServerError
		ctx.Status = status
		http.Error(ctx.W, err.Error(), status)
		return
	}

	ctx.Status = 200
	ctx.W.WriteHeader(200)
}
