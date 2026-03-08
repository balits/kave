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
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	reg        *prometheus.Registry
	server     *http.Server
}

func NewHTTPServer(
	httpPort string,
	kvService service.KVService,
	clusterService service.ClusterService,
	peerService service.PeerService,
	cfg *config.Config,
	reg *prometheus.Registry,
	logger *slog.Logger,
) *HttpServer {
	addr := "0.0.0.0:" + httpPort
	mux := http.NewServeMux()
	s := &HttpServer{
		kvSvc:      kvService,
		clusterSvc: clusterService,
		peerSvc:    peerService,
		reg:        reg,
		logger:     logger.With("component", "http_server", "addr", addr),
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	// kv
	mux.HandleFunc("GET "+transport.UriKvUri+"/get", s.handleGet)
	mux.HandleFunc("POST "+transport.UriKvUri+"/put", s.handlePut)
	mux.HandleFunc("DELETE "+transport.UriKvUri+"/delete", s.handleDelete)

	// cluster
	mux.HandleFunc("POST "+transport.UriCluster+"/join", s.handleJoin)

	// stats
	mux.HandleFunc("GET /stats", s.handleStats)

	// prometheus metrics
	mux.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{
		Registry: reg,
	}))

	// k8s
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

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

func (s *HttpServer) handleLivez(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received READYZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.logger.Error("Readiness check failed", "error", "raft is shutdown")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status": "raft shutdown"}`))
		return
	}

	if err := s.kvSvc.Ping(); err != nil {
		s.logger.Error("Readiness check failed", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status": "kv store ping failed: %v"}`, err)
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	}
}

func (s *HttpServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received LIVEZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.logger.Error("Readiness check failed", "error", "raft is shutdown")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status": "raft shutdown"}`))
		return
	}

	_, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status": "failed to get leader info: %v"}`, err)
		return
	}

	if err := s.peerSvc.LaggingBehind(); err != nil {
		s.logger.Error("Readiness check failed", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status": "raft is lagging behind: %v"}`, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "ok"}`))
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
