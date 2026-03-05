package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/transport"
)

const (
	getErrMsg        string = "failed to get key-value pair"
	setErrMsg        string = "failed to set key-value pair"
	deleteErrMsg     string = "failed to delete key-value pair"
	addClusterErrMsg string = "failed to add peer to the cluster"

	jsonEncodeErrMsg string = "failed to encode JSON body"
	jsonDecodeErrMsg string = "failed to decode JSON body"
)

// TODO: need to attach valid headers to results on the error path in handlers: maybe add KVStore to ClusterService and call clusterSvc.Meta()
// also TODO: sometimes http responses have header sometiems no, sometiems they have error sometimes not
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
	mux.HandleFunc("GET "+transport.UriKvUri+"/get", s.handleRange)
	mux.HandleFunc("POST "+transport.UriKvUri+"/put", s.handlePut)
	mux.HandleFunc("DELETE "+transport.UriKvUri+"/delete", s.handleDelete)

	// cluster
	mux.HandleFunc("POST "+transport.UriCluster+"/join", s.handleJoin)

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

func (s *HttpServer) handleRange(w http.ResponseWriter, r *http.Request) {
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var cmd kv.RangeCmd
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	if err := cmd.Check(); err != nil {
		s.logger.Error("Invalid request body", "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Range(r.Context(), cmd)
	if err != nil {
		s.logger.Error(getErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", getErrMsg, err), http.StatusInternalServerError)
		return
	}
	result.Header.NodeID = s.peerSvc.Me().NodeID // setting nodeID, since some reads may not go through raft (and get their header set perfectly by our fsm)
	s.writeJSON(w, *result, http.StatusOK)
}

func (s *HttpServer) handlePut(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received PUT request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var cmd kv.PutCmd
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}
	if err := cmd.Check(); err != nil {
		s.logger.Error("Invalid request body", "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Put(r.Context(), cmd)
	if err != nil {
		s.logger.Error(setErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", setErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, *result, http.StatusOK)
}

func (s *HttpServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received DELETE request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var cmd kv.DeleteCmd
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		s.logger.Error("Failed to decode request body", "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}
	if err := cmd.Check(); err != nil {
		s.logger.Error("Invalid request body", "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Delete(r.Context(), cmd)
	if err != nil {
		s.logger.Error(deleteErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", deleteErrMsg, err)
		writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, *result, http.StatusOK)
}

func (s *HttpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received JOIN request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		bytes, _ := json.Marshal(map[string]string{"error": fmt.Sprintf("failed to get leader info: %v", err)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req transport.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error("Failed to decode request body", "error", err)
		bytes, _ := json.Marshal(map[string]string{"error": fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes)
		return
	}

	err = s.clusterSvc.AddToCluster(r.Context(), req)
	if err != nil {
		s.logger.Error(addClusterErrMsg, "error", err)
		bytes, _ := json.Marshal(map[string]string{"error": fmt.Sprintf("%s: %v", addClusterErrMsg, err)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
	} else {
		s.logger.Info("Peer added to cluster successfully")
		bytes, _ := json.Marshal(map[string]string{"message": "Peer added to cluster successfully"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(bytes)
	}
}

func (s *HttpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received STATS request")
	stats, err := s.clusterSvc.Stats()
	if err != nil {
		s.logger.Error("Failed to get stats", "error", err)
		bytes, _ := json.Marshal(map[string]string{"error": fmt.Sprintf("failed to get stats: %v", err)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
	} else {
		bytes, _ := json.Marshal(stats)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(bytes)
	}
}

func (s *HttpServer) writeJSON(w http.ResponseWriter, result kv.Result, status int) {
	w.Header().Set("Content-Type", "application/json")
	bytes, err := json.Marshal(result)
	if err != nil {
		s.logger.Error("Failed to marshal JSON response", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(jsonEncodeErrMsg + ": " + err.Error()))
		return
	}

	w.WriteHeader(status)
	w.Write(bytes)
}

// TODO:
func writeError(w http.ResponseWriter, err string, status int) {
	w.Header().Set("Content-Type", "application/json")
	bytes, _ := json.Marshal(map[string]string{"error": err})
	w.WriteHeader(status)
	w.Write(bytes)
}

// TODO: need kubernetes for this to progress
func (s *HttpServer) redirectToLeader(w http.ResponseWriter, r *http.Request, leader config.Peer) {
	me := s.peerSvc.Me()
	if leader.NodeID == me.NodeID {
		s.logger.Debug("Redirecting to leader failed: we are the leader", "leader_id", leader.NodeID)
		return
	}

	s.logger.Debug("Redirecting to leader", "leader_id", leader.NodeID)
	w.WriteHeader(http.StatusTemporaryRedirect)
	bytes, _ := json.Marshal(map[string]string{
		"message":   "redirecting to leader",
		"leader_id": leader.NodeID,
	})
	w.Write(bytes)
}
