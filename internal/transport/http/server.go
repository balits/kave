package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/transport"
	"github.com/balits/kave/internal/types/api"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	kvGetErrMsg    string = "failed to get key-value pair"
	kvSetErrMsg    string = "failed to set key-value pair"
	kvDeleteErrMsg string = "failed to delete key-value pair"

	leaseGrantErrMsg     string = "failed to grant lease"
	leaseRevokeErrMsg    string = "failed to revoke lease"
	leaseKeepAliveErrMsg string = "failed to keep lease alive"
	leaseLookupErrMsg    string = "failed to lookup lease"

	addClusterErrMsg string = "failed to add peer to the cluster"

	jsonEncodeErrMsg string = "failed to encode JSON body"
	jsonDecodeErrMsg string = "failed to decode JSON body"
)

// TODO: need to attach valid headers to results on the error path in handlers: maybe add KVStore to ClusterService and call clusterSvc.Meta()
// also TODO: sometimes http responses have header sometiems no, sometiems they have error sometimes not
type HttpServer struct {
	kvSvc      service.KVService
	leaseSvc   service.LeaseService
	clusterSvc service.ClusterService
	peerSvc    service.PeerService
	logger     *slog.Logger
	server     *http.Server
}

func NewHTTPServer(
	logger *slog.Logger,
	httpPort string,
	kvService service.KVService,
	leaseService service.LeaseService,
	clusterService service.ClusterService,
	peerService service.PeerService,
	cfg *config.Config,
	reg *prometheus.Registry,
) *HttpServer {
	addr := "0.0.0.0:" + httpPort
	mux := http.NewServeMux()
	s := &HttpServer{
		kvSvc:      kvService,
		leaseSvc:   leaseService,
		clusterSvc: clusterService,
		peerSvc:    peerService,
		logger:     logger.With("component", "http_server", "addr", addr),
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	// kv
	mux.HandleFunc("GET "+transport.UriKv+"/get", s.handleKvGet)
	mux.HandleFunc("POST "+transport.UriKv+"/put", s.handleKvPut)
	mux.HandleFunc("DELETE "+transport.UriKv+"/delete", s.handleKvDelete)
	mux.HandleFunc("POST "+transport.UriKv+"/txn", s.handleKvTxn)

	// lease
	mux.HandleFunc("POST "+transport.UriLease+"/grant", s.handleLeaseGrant)
	mux.HandleFunc("DELETE "+transport.UriLease+"/revoke", s.handleLeaseRevoke)
	mux.HandleFunc("POST "+transport.UriLease+"/keep-alive", s.handleLeaseKeepAlive)
	mux.HandleFunc("POST "+transport.UriLease+"/lookup", s.handleLeaseLookup)

	// cluster
	mux.HandleFunc("POST "+transport.UriCluster+"/join", s.handleJoin)

	// stats
	mux.HandleFunc("GET /stats", s.handleStats)

	// prometheus metrics
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

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

func (s *HttpServer) handleKvGet(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_GET request")

	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.RangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Range(r.Context(), req)
	if err != nil {
		s.logger.Error(kvGetErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", kvGetErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, result, http.StatusOK)
}

func (s *HttpServer) handleKvPut(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_PUT request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Put(r.Context(), req)
	if err != nil {
		s.logger.Error(kvSetErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", kvSetErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, *result, http.StatusOK)
}

func (s *HttpServer) handleKvDelete(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_DELETE request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Delete(r.Context(), req)
	if err != nil {
		s.logger.Error(kvDeleteErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", kvDeleteErrMsg, err)
		writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, *result, http.StatusOK)
}

func (s *HttpServer) handleKvTxn(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_TXN request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.TxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.kvSvc.Txn(r.Context(), req)
	if err != nil {
		s.logger.Error(kvDeleteErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", kvDeleteErrMsg, err)
		writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, *result, http.StatusOK)
}

func (s *HttpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received CLUSTER_JOIN request")
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

func (s *HttpServer) handleLeaseGrant(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_GRANT request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.LeaseGrantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.leaseSvc.Grant(r.Context(), req)
	if err != nil {
		s.logger.Error(leaseGrantErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", kvSetErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseGrant: result}, http.StatusOK)
}
func (s *HttpServer) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_REVOKE request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.LeaseRevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.leaseSvc.Revoke(r.Context(), req)
	if err != nil {
		s.logger.Error(leaseRevokeErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", kvSetErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseRevoke: result}, http.StatusOK)
}

func (s *HttpServer) handleLeaseKeepAlive(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_KEEP_ALIVE request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.LeaseKeepAliveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.leaseSvc.KeepAlive(r.Context(), req)
	if err != nil {
		s.logger.Error(leaseKeepAliveErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", kvSetErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseKeepAlive: result}, http.StatusOK)
}

func (s *HttpServer) handleLeaseLookup(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_LOOKUP request")
	leader, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusInternalServerError)
		return
	}

	me := s.peerSvc.Me()
	if leader.NodeID != me.NodeID {
		s.redirectToLeader(w, r, leader)
		return
	}

	var req api.LeaseLookupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.leaseSvc.Lookup(r.Context(), req)
	if err != nil {
		s.logger.Error(leaseLookupErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", kvSetErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseLookup: result}, http.StatusOK)
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

func (s *HttpServer) writeJSON(w http.ResponseWriter, response any, status int) {
	w.Header().Set("Content-Type", "application/json")
	bytes, err := json.Marshal(response)
	if err != nil {
		s.logger.Error("Failed to marshal JSON response", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(jsonEncodeErrMsg + ": " + err.Error()))
		return
	}

	w.WriteHeader(status)
	w.Write(bytes)
}

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
