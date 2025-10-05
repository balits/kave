package node

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/balits/thesis/store"
	"github.com/hashicorp/raft"
)

const errInternalServerErrorMsg string = "internal server error"

// StartServer starts the http server
func (n *Node) StartServer() error {
	err := n.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		n.logger.Error("HTTP Server failed", "error", err)
		return err
	}

	return nil
}

// ShutdownServer terminates the http server with the supplied timeout
func (n *Node) ShutdownServer(timeout time.Duration) error {
	c, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := n.server.Shutdown(c)
	return err
}

// newMux registers different handlers for the public facing api routes and returns the new multiplexer
func newMux(node *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/set", setHandler(node))
	mux.HandleFunc("/get", getHandler(node))
	mux.HandleFunc("/delete", deleteHandler(node))
	return mux
}

func getHandler(node *Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			check(respondError(w, "Method not allowed", "", http.StatusMethodNotAllowed))
			return
		}
		node.logger.Debug("HTTP /get request initiated")

		var body struct {
			Key string `json:"key"`
		}

		defer r.Body.Close()
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			check(respondError(w, "Bad request", "", http.StatusMethodNotAllowed))
		}

		value, err := node.store.GetStale(body.Key)
		switch err {
		case nil:
			node.logger.Debug("HTTP /get request", "key", body.Key, "value", value)
			data := map[string]string{"value": value}
			check(respond(w, data, "", "", http.StatusOK))
		case store.ErrorKeyNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
			check(respondError(w, err.Error(), "", http.StatusNotFound))
		default:
			check(respondInternalServerError(w, err.Error()))
		}
	}
}

func setHandler(node *Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			check(respondError(w, "Method not allowed", "", http.StatusMethodNotAllowed))
			return
		}
		node.logger.Debug("HTTP /set request initiated")

		// fixme: move to reverse proxy + internal redirection
		if node.raft.State() != raft.Leader {
			http.Error(w, "Not leader", http.StatusBadRequest)
			url := "http://" + string(node.raft.Leader()) + "/set"
			http.Redirect(w, r, url, http.StatusTemporaryRedirect)
			return
		}

		var body struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}

		defer r.Body.Close()
		err := json.NewDecoder(r.Body).Decode(&body)
		node.logger.Debug("HTTP /set body parsed", "body", body, "error", err)

		if err != nil || body.Key == "" || body.Value == "" {
			check(respondError(w, "Bad request: missing or invalid fields", "", http.StatusBadRequest))
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
			check(respondInternalServerError(w, err.Error()))
			return
		}

		future := node.raft.Apply(buff.Bytes(), 10*time.Second)
		err = future.Error()
		if err != nil {
			check(respondInternalServerError(w, err.Error()))
			return
		}

		applyResponse := future.Response().(ApplyResponse)
		if applyResponse.IsError() {
			check(respondError(w, applyResponse.err.Error(), "", http.StatusInternalServerError))
			return
		}

		check(respond(w, nil, "", "", http.StatusOK))
	}
}

func deleteHandler(node *Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			check(respondError(w, "Method not allowed", "", http.StatusMethodNotAllowed))
			return
		}

		node.logger.Debug("HTTP /delete request initiated")
		var body struct {
			Key string `json:"key"`
		}

		defer r.Body.Close()
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			check(respondError(w, "Bad request", "", http.StatusMethodNotAllowed))
		}

		var buff bytes.Buffer
		cmd := store.Cmd{
			Kind: store.CmdKindDelete,
			Key:  body.Key,
		}
		err = gob.NewEncoder(&buff).Encode(cmd)
		if err != nil {
			check(respondInternalServerError(w, err.Error()))
			return
		}

		future := node.raft.Apply(buff.Bytes(), time.Second*5)
		err = future.Error()
		if err != nil {
			check(respondInternalServerError(w, err.Error()))
			return
		}

		applyResponse := future.Response().(ApplyResponse)
		if applyResponse.IsError() {
			check(respondError(w, applyResponse.err.Error(), "", http.StatusInternalServerError))
			return
		}

		data := map[string]string{"value": applyResponse.cmd.Value}
		check(respond(w, data, "", "", http.StatusOK))
	}
}

// HttpResponse is a unified, consistent response type that all api routes should return
type HttpResponse struct {
	Data    any    `json:"data"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

func respond(w http.ResponseWriter, data any, msg string, errText string, statusCode int) error {
	r := HttpResponse{
		Data:    data,
		Message: msg,
		Error:   errText,
	}

	bytes, err := json.Marshal(r)
	if err != nil {
		return err
	}

	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Option", "nosniff")
	w.WriteHeader(statusCode)
	w.Write(bytes)
	return nil
}

// respondError responds with an error variant of `HttpResponse`: it does not contain data, only a non-nil error and an optional message
func respondError(w http.ResponseWriter, errText string, msg string, statusCode int) error {
	var e string
	if errText == "" {
		e = errInternalServerErrorMsg
	} else {
		e = errText
	}

	return respond(w, nil, msg, e, statusCode)
}

func respondInternalServerError(w http.ResponseWriter, errorText string) error {
	var e string
	if errorText != "" {
		e = fmt.Sprintf("%s\n%s\n", errInternalServerErrorMsg, errorText)
	} else {
		e = errInternalServerErrorMsg
	}
	return respondError(w, e, "", http.StatusInternalServerError)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
