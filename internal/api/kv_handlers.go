package api

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

const (
	errMissingOrInvalidFieldsOnRequestBody string = "request body: missing or invalid fields"
	errRaftInstanceNotSetup                string = "raft instance is not yet set up"
	errApplyResponseCastFailed             string = "failed to cast raft's response to ApplyResponse"
)

func (s *Server) getHandler(ctx *web.Context) {
	if s.node.Raft == nil {
		ctx.Error("Raft instance is not yet set up", http.StatusServiceUnavailable)
		return
	}

	var body web.GetBody
	err := ctx.ReadJSON(&body)
	if err != nil {
		ctx.Error(err.Error(), http.StatusBadRequest)
		return
	} else if body.Key == "" {
		ctx.Error(errMissingOrInvalidFieldsOnRequestBody, http.StatusBadRequest)
		return
	}

	value, err := s.node.GetStore().GetStale(body.Key)
	switch err {
	case nil:
		s.Logger.Debug("HTTP /get request", "key", body.Key, "value", value)
		data := map[string][]byte{"value": value}
		ctx.Ok(data)
	case store.ErrorKeyNotFound:
		s.Logger.Debug("HTTP /get request: key not found", "key", body.Key)
		ctx.Error("Key not found", http.StatusNotFound)
	default:
		ctx.Error(fmt.Sprintf("Error during store.Get: %v", err.Error()), http.StatusInternalServerError)
	}
}

func (s *Server) setHandler(ctx *web.Context) {
	if s.node.Raft == nil {
		ctx.Error(errRaftInstanceNotSetup, http.StatusServiceUnavailable)
		return
	}
	// FIXME: move to reverse proxy + internal redirection
	if s.node.Raft.State() != raft.Leader {
		var currentLeader *config.Peer
		leaderAddr, leaderID := s.node.Raft.LeaderWithID()
		s.Logger.Debug("Leader", "leader address", leaderAddr, "leader id", leaderID)
		if leaderAddr == "" || leaderID == "" {
			ctx.Error("no current leader found", http.StatusInternalServerError)
			return
		}
		for _, server := range s.node.Config.Peers {
			if server.GetRaftAddress() == leaderAddr {
				currentLeader = &server
			}
		}
		if currentLeader == nil {
			ctx.Error("http address of the leader was not found", http.StatusInternalServerError)
			return
		}
		s.Logger.Debug("Redirecting to leader", "leader", fmt.Sprintf("%+v", currentLeader))
		url := "http://" + currentLeader.GetInternalHttpAddress() + "/set"
		http.Redirect(ctx.W, ctx.R, url, http.StatusTemporaryRedirect)
		return
	}

	var body web.SetBody

	err := ctx.ReadJSON(&body)
	if err != nil {
		ctx.Error(err.Error(), http.StatusBadRequest)
		return
	} else if body.Key == "" || body.Value == nil {
		ctx.Error(errMissingOrInvalidFieldsOnRequestBody, http.StatusBadRequest)
		return
	}

	var buff bytes.Buffer
	cmd := store.Cmd{
		Kind:  store.CmdKindSet,
		Key:   body.Key,
		Value: body.Value,
	}
	err = gob.NewEncoder(&buff).Encode(cmd)
	if err != nil {
		ctx.Error(fmt.Sprintf("Error during gob encoding: %v", err.Error()), http.StatusInternalServerError)
		return
	}

	future := s.node.Raft.Apply(buff.Bytes(), 5*time.Second)
	err = future.Error()
	if err != nil {
		ctx.Error(fmt.Sprintf("Error during raft.Apply: %v", err.Error()), http.StatusInternalServerError)
		return
	}

	applyResponse, ok := future.Response().(store.ApplyResponse)
	if !ok {
		ctx.Error(errApplyResponseCastFailed, http.StatusInternalServerError)
		return
	}
	if applyResponse.IsError() {
		ctx.Error(fmt.Sprintf("Error during raft.Apply.Response: %v", applyResponse.GetError()), http.StatusInternalServerError)
		return
	}
	ctx.Ok(nil)
}

func (s *Server) deleteHandler(ctx *web.Context) {
	if s.node.Raft == nil {
		ctx.Error(errRaftInstanceNotSetup, http.StatusServiceUnavailable)
		return
	}

	var body web.GetBody
	err := ctx.ReadJSON(&body)
	if err != nil {
		ctx.Error(fmt.Sprintf("Request body: %v", err.Error()), http.StatusBadRequest)
		return
	} else if body.Key == "" {
		ctx.Error(errMissingOrInvalidFieldsOnRequestBody, http.StatusBadRequest)
		return
	}

	var buff bytes.Buffer
	cmd := store.Cmd{
		Kind: store.CmdKindDelete,
		Key:  body.Key,
	}
	err = gob.NewEncoder(&buff).Encode(cmd)
	if err != nil {
		ctx.Error(fmt.Sprintf("Error during gob encoding: %v", err.Error()), http.StatusInternalServerError)
		return
	}

	future := s.node.Raft.Apply(buff.Bytes(), time.Second*5)
	err = future.Error()
	if err != nil {
		ctx.Error(fmt.Sprintf("Error during raft.Apply: %v", err.Error()), http.StatusInternalServerError)
		return
	}

	applyResponse := future.Response().(store.ApplyResponse)
	if applyResponse.IsError() {
		ctx.Error(fmt.Sprintf("Error during raft.Apply.Response: %v", applyResponse.GetError()), http.StatusInternalServerError)
		return
	}
	data := map[string][]byte{"value": applyResponse.Cmd.Value}
	ctx.Ok(data)
}
