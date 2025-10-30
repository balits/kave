package api

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

const (
	errMissingOrInvalidFieldsOnRequestBody string = "Request body: missing or invalid fields"
	errRaftInstanceNotSetup                string = "Raft instance is not yet set up"
	errApplyResponseCastFailed             string = "Failed to cast raft's response to ApplyResponse"
)

func (s *Server) getHandler(ctx *web.Context) {
	var body struct {
		Key string `json:"key"`
	}

	if s.node.Raft == nil {
		ctx.Error("Raft instance is not yet set up", http.StatusServiceUnavailable)
		return
	}

	err := ctx.ReadJSON(&body)
	if err != nil {
		ctx.Error(err.Error(), http.StatusBadRequest)
		return
	} else if body.Key == "" {
		ctx.Error(errMissingOrInvalidFieldsOnRequestBody, http.StatusBadRequest)
		return
	}

	value, err := s.node.Store.GetStale(body.Key)
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
		var currentLeader *config.ServiceInfo
		leaderAddr, leaderID := s.node.Raft.LeaderWithID()
		s.Logger.Debug("Leader", "leader address", leaderAddr, "leader id", leaderID)
		if leaderAddr == "" || leaderID == "" {
			ctx.Error("no current leader found", http.StatusInternalServerError)
			return
		}
		for _, server := range s.node.Config.ClusterInfo {
			if server.RaftID == string(leaderID) {
				currentLeader = &server
			}
		}
		if currentLeader == nil {
			ctx.Error("http address of the leader was not found", http.StatusInternalServerError)
			return
		}
		s.Logger.Debug("Redirecting to leader", "leader", fmt.Sprintf("%+v", currentLeader))
		url := "http://" + currentLeader.HttpHost + ":" + currentLeader.ExternalHttpPort + "/set"
		http.Redirect(ctx.W, ctx.R, url, http.StatusTemporaryRedirect)
		return
	}

	var body struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}

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

	var body struct {
		Key string `json:"key"`
	}

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
	data := map[string][]byte{"value": applyResponse.Cmd().Value}
	ctx.Ok(data)
}

type joinBody struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func (s *Server) joinHandler(ctx *web.Context) {
	if s.node.Raft.State() != raft.Leader {
		http.Error(ctx.W, "not leader", http.StatusConflict)
		return
	}

	var body joinBody
	err := ctx.ReadJSON(&body)
	if err != nil {
		http.Error(ctx.W, err.Error(), http.StatusBadRequest)
		return
	} else if body.Addr == "" || body.ID == "" {
		http.Error(ctx.W, errMissingOrInvalidFieldsOnRequestBody, http.StatusBadRequest)
		return
	}

	f := s.node.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		http.Error(ctx.W, fmt.Sprintf("failed to read configuration: %v", err), http.StatusInternalServerError)
		return
	}
	// TODO: what are we doing here?
	// we check if the provided serverID is in the cluster configuration
	// but if it is then we just return OK?
	// i mean provided that only real nodes try to connect, they are gonne be in the cluster config
	// bcs we assemble said config in service.Bootstrap() from the config file
	// or mabye they only get added as real voters of the cluster once joinHandler succeeds,
	// then no other join request should go to the line "s.Raft.AddVoter(...)" so we return early?
	raftConfig := f.Configuration()
	for _, node := range raftConfig.Servers {
		if node.ID == raft.ServerID(body.ID) {
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

func TryJoin(me config.ServiceInfo, urls []string, timeout time.Duration) error {
	body := joinBody{
		ID:   me.RaftID,
		Addr: me.GetRaftAddress(),
	}
	bodyBytes, _ := json.Marshal(body)
	client := http.Client{Timeout: timeout}
	var lastErr error

	backoff := []int{
		1, 2, 4, 16,
	}
	fmt.Println("try joining target urls: ", urls)
	for _, b := range backoff {
		for _, url := range urls {
			req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
			if err != nil {
				lastErr = fmt.Errorf("could not create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				lastErr = fmt.Errorf("could not send request: %w", err)
				continue
			}

			defer resp.Body.Close()
			buf := make([]byte, 1024)
			n, _ := resp.Body.Read(buf)
			respBody := string(buf[:n])

			// fmt.Printf("round %d, attempt %d: url=%s, status=%s, ", round, attempt, url, resp.Status)
			switch resp.StatusCode {
			case http.StatusOK:
				fmt.Println("joined succesfully")
				return nil
			case http.StatusNoContent:
				fmt.Println("already joined succesfully")
				return nil
			case http.StatusConflict: // not leader
				fmt.Println("node was not the leader")
				lastErr = errors.Join(lastErr, fmt.Errorf("not leader"))
			case http.StatusBadRequest:
				fmt.Printf("err=%s\n", respBody)
				lastErr = errors.Join(lastErr, fmt.Errorf("bad request"))
			case http.StatusServiceUnavailable:
				fmt.Println("raft not set up yet")
				lastErr = errors.Join(lastErr, fmt.Errorf("raft not set up yet"))
			default:
				s := fmt.Sprint("something wrong here", resp.Status)
				lastErr = errors.Join(lastErr, errors.New(s))
			}
		}
		fmt.Printf("retrying in %d seconds. Last error: %v\n", b, lastErr)
		time.Sleep(time.Duration(b) * time.Second)
	}

	fmt.Printf("No attempt was successful. Last error: %v\n", lastErr)
	return lastErr
}
