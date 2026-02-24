package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/common"
	"github.com/balits/kave/internal/service"
)

const (
	getErrMsg        string = "failed to get key-value pair"
	setErrMsg        string = "failed to set key-value pair"
	deleteErrMsg     string = "failed to delete key-value pair"
	addClusterErrMsg string = "failed to add peer to the cluster"

	jsonEncodeErrMsg string = "failed to encode JSON body"
	jsonDecodeErrMsg string = "failed to decode JSON body"
)

type HttpServer struct {
	kvSvc      service.KVService
	clusterSvc service.ClusterService
	peerSvc    service.PeerService
	logger     *slog.Logger
	server     *http.Server
}

func NewHTTPServer(
	addr string,
	kvService service.KVService,
	clusterService service.ClusterService,
	peerService service.PeerService,
	cfg *config.Config,
	logger *slog.Logger,
) *HttpServer {
	mux := http.NewServeMux()
	s := &HttpServer{
		kvSvc:      kvService,
		clusterSvc: clusterService,
		peerSvc:    peerService,
		logger:     logger.With("component", "http_server", "addr", addr),
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	// kv
	mux.HandleFunc("GET "+common.UriKvUri+"/get", s.handleGet)
	mux.HandleFunc("POST "+common.UriKvUri+"/set", s.handleSet)
	mux.HandleFunc("DELETE "+common.UriKvUri+"/delete", s.handleDelete)

	// cluster
	mux.HandleFunc("POST "+common.UriCluster+"/join", s.handleJoin)

	mux.HandleFunc("GET /stats", s.handleStats)

	return s
}

func (s *HttpServer) Start() error {
	s.logger.Info("Starting HTTP server", "addr", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *HttpServer) Shutdown(ctx context.Context) error {
	s.logger.Info("Stopping HTTP server")
	s.server.SetKeepAlivesEnabled(false)
	if err := s.server.Shutdown(ctx); err != nil {
		s.server.Close()
		return err
	}
	return nil
}

func (s *HttpServer) handleGet(w http.ResponseWriter, r *http.Request) {
	var req common.GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Errorf("%s: %v", jsonDecodeErrMsg, err)
		s.writeJSON(w, Response{Error: err}, http.StatusBadRequest)
		return
	}

	var (
		response Response
		status   int
	)

	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		s.writeJSON(w, Response{Error: err}, http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	result, err := s.kvSvc.Get(r.Context(), req)
	if err != nil {
		s.logger.Error(getErrMsg, "error", err)
		response = Response{Error: fmt.Errorf("%s: %v", getErrMsg, err)}
		status = http.StatusInternalServerError
		return
	} else {
		response = Response{Result: result}
		status = http.StatusOK
	}

	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleSet(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received SET request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		s.writeJSON(w, Response{Error: err}, http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req common.SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Errorf("%s: %v", jsonDecodeErrMsg, err)
		s.writeJSON(w, Response{Error: err}, http.StatusBadRequest)
		return
	}

	var (
		status   int
		response Response
	)
	result, err := s.kvSvc.Set(r.Context(), req)

	if err != nil {
		s.logger.Error(setErrMsg, "error", err)
		response = Response{Error: err}
		status = http.StatusInternalServerError
	} else {
		response = Response{Result: result}
		status = http.StatusOK
	}

	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received DELETE request")

	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		s.writeJSON(w, Response{Error: err}, http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req common.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error("Failed to decode request body", "error", err)
		err := fmt.Errorf("%s: %v", jsonDecodeErrMsg, err)
		s.writeJSON(w, Response{Error: err}, http.StatusBadRequest)
		return
	}

	resp, err := s.kvSvc.Delete(r.Context(), req)
	if err != nil {
		s.logger.Error(deleteErrMsg, "error", err)
		err := fmt.Errorf("%s: %v", deleteErrMsg, err)
		s.writeJSON(w, Response{Error: err}, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, Response{Result: resp}, http.StatusOK)
}

func (s *HttpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received JOIN request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		s.writeJSON(w, Response{Error: err}, http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req common.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error("Failed to decode request body", "error", err)
		err := fmt.Errorf("%s: %v", jsonDecodeErrMsg, err)
		s.writeJSON(w, Response{Error: err}, http.StatusBadRequest)
		return
	}

	var (
		status   int
		response Response
	)
	err = s.clusterSvc.AddToCluster(r.Context(), req)
	if err != nil {
		s.logger.Error(addClusterErrMsg, "error", err)
		response = Response{Error: err}
		status = http.StatusInternalServerError
	} else {
		response = Response{Result: "Peer added to cluster successfully"}
		status = http.StatusOK
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received STATS request")
	stats, err := s.clusterSvc.Stats()
	if err != nil {
		s.logger.Error("Failed to get stats", "error", err)
		s.writeJSON(w, Response{Error: err}, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, Response{Result: stats}, http.StatusOK)
}

func (s *HttpServer) writeJSON(w http.ResponseWriter, data Response, status int) {
	w.Header().Set("Content-Type", "application/json")
	bytes, err := json.Marshal(data)
	if err != nil {
		s.logger.Error("Failed to marshal JSON response", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(jsonEncodeErrMsg + ": " + err.Error()))
		return
	}

	data.Header = s.clusterSvc.Meta()
	w.WriteHeader(status)
	w.Write(bytes)
}

func (s *HttpServer) redirectToLeader(w http.ResponseWriter, r *http.Request, leader common.Peer) {
	me := s.peerSvc.Me()
	if leader.NodeID == me.NodeID {
		s.logger.Debug("Redirecting to leader failed: we are the leader", "leader_id", leader.NodeID, "leader_http_addr", leader.GetPublicHttpAddress())
		return
	}

	s.logger.Debug("Redirecting to leader", "leader_id", leader.NodeID, "leader_http_addr", leader.GetPublicHttpAddress())
	w.Header().Set("Location", "http://"+leader.GetPublicHttpAddress()+r.RequestURI)
	s.writeJSON(w, Response{Result: leader, Error: fmt.Errorf("redirecting to leader")}, http.StatusTemporaryRedirect)
}
