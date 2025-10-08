package node

import (
	"bytes"
	"encoding/gob"
	"net/http"
	"time"

	"github.com/balits/thesis/config"
	"github.com/balits/thesis/store"
	"github.com/balits/thesis/web"
	"github.com/hashicorp/raft"
)

const errMissingOrInvalidFields string = "missing or invalid fields"

func (n *Node) RegisterRoutes() {
	n.server.Router.Register("GET", "/get", n.getHandler)
	n.server.Router.Register("POST", "/set", n.setHandler)
	n.server.Router.Register("DELETE", "/delete", n.deleteHandler)
}

// StartServer starts the http server
func (n *Node) StartServer() error {
	n.logger.Info("HTTP Server started", "address", config.Config.HttpAddr)
	return n.server.Start()
}

// ShutdownServer terminates the http server with the supplied timeout
func (n *Node) ShutdownServer(timeout time.Duration) error {
	return n.server.Shutdown(timeout)
}

func (node *Node) getHandler(ctx *web.Context) {
	var response *web.ResponseData
	var body struct {
		Key string `json:"key"`
	}

	err := ctx.ReadJSON(&body)
	if err != nil {
		response = web.NewResponseData(nil, "", errMissingOrInvalidFields)
		ctx.Respond(response, http.StatusBadRequest)
		return
	}

	value, err := node.store.GetStale(body.Key)
	switch err {
	case nil:
		node.logger.Debug("HTTP /get request", "key", body.Key, "value", value)
		response = web.NewResponseData(map[string][]byte{"value": value}, "", "")
		ctx.Respond(response, http.StatusOK)
	case store.ErrorKeyNotFound:
		node.logger.Debug("HTTP /get request: key not found", "key", body.Key)
		response = web.NewResponseData(nil, "", "Key not found")
		ctx.Respond(response, http.StatusNotFound)
	default:
		response = web.NewResponseData(nil, "", err.Error())
		ctx.Respond(response, http.StatusInternalServerError)
	}
}

func (node *Node) setHandler(ctx *web.Context) {
	var response *web.ResponseData

	// fixme: move to reverse proxy + internal redirection
	if node.raft.State() != raft.Leader {
		response = web.NewResponseData(nil, "", "Node is not the leader, redirecting")
		ctx.Respond(response, http.StatusBadRequest)
		url := "http://" + string(node.raft.Leader()) + "/set"
		http.Redirect(ctx.W, ctx.R, url, http.StatusTemporaryRedirect)
		return
	}

	var body struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}

	err := ctx.ReadJSON(&body)
	node.logger.Debug("HTTP /set body parsed", "body", body, "error", err)

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

	future := node.raft.Apply(buff.Bytes(), 10*time.Second)
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

func (node *Node) deleteHandler(ctx *web.Context) {
	var response *web.ResponseData
	var body struct {
		Key string `json:"key"`
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

	future := node.raft.Apply(buff.Bytes(), time.Second*5)
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
