package service

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/config"
	"github.com/balits/thesis/store"
	"github.com/balits/thesis/web"
	"github.com/hashicorp/raft"
)

const errMissingOrInvalidFields string = "missing or invalid fields"

func (s *Service) RegisterRoutes() {
	s.Server.Router.Register("GET", "/get", s.getHandler)
	s.Server.Router.Register("POST", "/set", s.setHandler)
	s.Server.Router.Register("DELETE", "/delete", s.deleteHandler)
	s.Server.Router.Register("POST", "/join", s.joinHandler)
}

func (s *Service) getHandler(ctx *web.Context) {
	var response *web.ResponseData
	var body struct {
		Key string `json:"key"`
	}

	if s.Raft == nil {
		response = web.NewResponseData(nil, "", "Raft instance is not yet set up")
		ctx.Respond(response, http.StatusServiceUnavailable)
		return
	}

	err := ctx.ReadJSON(&body)
	if err != nil {
		response = web.NewResponseData(nil, "", errMissingOrInvalidFields)
		ctx.Respond(response, http.StatusBadRequest)
		return
	}

	value, err := s.Store.GetStale(body.Key)
	switch err {
	case nil:
		s.Logger.Debug("HTTP /get request", "key", body.Key, "value", value)
		response = web.NewResponseData(map[string][]byte{"value": value}, "", "")
		ctx.Respond(response, http.StatusOK)
	case store.ErrorKeyNotFound:
		s.Logger.Debug("HTTP /get request: key not found", "key", body.Key)
		response = web.NewResponseData(nil, "", "Key not found")
		ctx.Respond(response, http.StatusNotFound)
	default:
		response = web.NewResponseData(nil, "", err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
	}
}

func (s *Service) setHandler(ctx *web.Context) {
	var response *web.ResponseData

	if s.Raft == nil {
		response = web.NewResponseData(nil, "", "Raft instance is not yet set up")
		ctx.Respond(response, http.StatusServiceUnavailable)
		return
	}
	// FIXME: move to reverse proxy + internal redirection
	if s.Raft.State() != raft.Leader {
		var currentLeader *config.ServiceInfo
		leaderAddr, leaderID := s.Raft.LeaderWithID()
		s.Logger.Debug("Leader", "leader address", leaderAddr, "leader id", leaderID)
		if leaderAddr == "" || leaderID == "" {
			ctx.Respond(web.NewResponseData(nil, "", "no current leader found"), http.StatusInternalServerError)
			return
		}
		for _, server := range s.Config.ClusterInfo {
			if server.RaftID == string(leaderID) {
				currentLeader = &server
			}
		}
		if currentLeader == nil {
			response = web.NewResponseData(nil, "", "http address of the leader was not found")
			ctx.Respond(response, http.StatusInternalServerError)
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
	s.Logger.Debug("HTTP /set body parsed", "body", body, "error", err)

	if err != nil || body.Key == "" || body.Value == nil {
		response = web.NewResponseData(nil, "", errMissingOrInvalidFields)
		ctx.Respond(response, http.StatusBadRequest)
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
		response = web.NewResponseData(nil, "", err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
		return
	}

	future := s.Raft.Apply(buff.Bytes(), 10*time.Second)
	err = future.Error()
	if err != nil {
		response = web.NewResponseData(nil, "", err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
		return
	}

	applyResponse := future.Response().(ApplyResponse)
	if applyResponse.IsError() {
		response = web.NewResponseData(nil, "", applyResponse.err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
		return
	}

	response = web.NewResponseData(nil, "", "")
	ctx.Respond(response, http.StatusOK)
}

func (s *Service) deleteHandler(ctx *web.Context) {
	var response *web.ResponseData
	var body struct {
		Key string `json:"key"`
	}

	if s.Raft == nil {
		response = web.NewResponseData(nil, "", "Raft instance is not yet set up")
		ctx.Respond(response, http.StatusServiceUnavailable)
		return
	}

	err := ctx.ReadJSON(&body)
	if err != nil || body.Key == "" {
		response = web.NewResponseData(nil, "", errMissingOrInvalidFields)
		ctx.Respond(response, http.StatusBadRequest)
		return
	}

	var buff bytes.Buffer
	cmd := store.Cmd{
		Kind: store.CmdKindDelete,
		Key:  body.Key,
	}
	err = gob.NewEncoder(&buff).Encode(cmd)
	if err != nil {
		response = web.NewResponseData(nil, "", err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
		return
	}

	future := s.Raft.Apply(buff.Bytes(), time.Second*5)
	err = future.Error()
	if err != nil {
		response = web.NewResponseData(nil, "", err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
		return
	}

	applyResponse := future.Response().(ApplyResponse)
	if applyResponse.IsError() {
		response = web.NewResponseData(nil, "", applyResponse.err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
		return
	}
	data := map[string][]byte{"value": applyResponse.cmd.Value}
	response = web.NewResponseData(data, "", "")
	ctx.Respond(response, http.StatusOK)
}

type joinBody struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func (s *Service) joinHandler(ctx *web.Context) {
	if s.Raft == nil {
		http.Error(ctx.W, "Raft instance is not yet set up", http.StatusServiceUnavailable)
		return
	}

	if s.Raft.State() != raft.Leader {
		http.Error(ctx.W, "not leader", http.StatusConflict) // signals retry
		return
	}

	var body joinBody

	err := ctx.ReadJSON(&body)
	if err != nil {
		http.Error(ctx.W, err.Error(), http.StatusBadRequest)
		return
	}

	f := s.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		http.Error(ctx.W, fmt.Sprintf("failed to read configuration: %v", err), http.StatusInternalServerError)
		return
	}
	raftConfig := f.Configuration()
	for _, node := range raftConfig.Servers {
		if node.ID == raft.ServerID(body.ID) {
			ctx.W.WriteHeader(http.StatusNoContent)
			return
		}
	}

	future := s.Raft.AddVoter(raft.ServerID(body.ID), raft.ServerAddress(body.Addr), 0, 5*time.Second)
	if err = future.Error(); err != nil {
		http.Error(ctx.W, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx.W.WriteHeader(http.StatusNoContent)
}

func tryJoin(cfg *config.Config, timeout time.Duration) error {
	me := cfg.ThisService
	var urls []string
	for _, i := range cfg.ClusterInfo {
		if i == *me {
			continue
		}
		//TODO: using rafthost + internal http port to dial other nodes works,
		// because rafthost is set to the service name defined in docker-compose, so it will be resolved through its own network
		// but this becomes a problem when we want to redirect an http request: where do we redirect it? we need to know the nodes external host + port
		// alternatively we set up a reverse proxy between the outside world and our cluster, load balancing each request equally between the nodes and:
		//   a) only serve writes through the leader and we need to track leader node changes (for reads we can have both stale reads and up to date reads)
		///  b) distribute request fairly, redirecting writes to the current leader (easy since a Raft instance knows the leader)
		urls = append(urls, "http://"+i.RaftHost+":"+i.InternalHttpPort+"/join")
	}

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
	// fmt.Println("target urls: ", urls)
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
			case http.StatusNoContent: // ok
				fmt.Println("joined succesfully")
				return nil
			case http.StatusConflict: // not leader
				fmt.Println("node was not the leader")
				continue
			case http.StatusBadRequest:
				fmt.Printf("err=%s\n", respBody)
				lastErr = errors.Join(lastErr, fmt.Errorf("bad request"))
			case http.StatusServiceUnavailable:
				fmt.Println("raft not set up yet")
				lastErr = errors.Join(lastErr, fmt.Errorf("raft not set up yet"))
			default:
				fmt.Println("something wrong here")
			}
		}
		fmt.Printf("No attempt was successful, retrying in %d seconds. Last error: %v\n", b, lastErr)
		time.Sleep(time.Duration(b) * time.Second)
	}

	return lastErr
}
