package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"

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

	// ops requiring a leader

	// kv
	mux.HandleFunc("GET "+transport.UriKv+"/get", s.leaderMiddleware(s.handleKvGet))
	mux.HandleFunc("POST "+transport.UriKv+"/put", s.leaderMiddleware(s.handleKvPut))
	mux.HandleFunc("DELETE "+transport.UriKv+"/delete", s.leaderMiddleware(s.handleKvDelete))
	mux.HandleFunc("POST "+transport.UriKv+"/txn", s.leaderMiddleware(s.handleKvTxn))
	// lease
	mux.HandleFunc("POST "+transport.UriLease+"/grant", s.leaderMiddleware(s.handleLeaseGrant))
	mux.HandleFunc("DELETE "+transport.UriLease+"/revoke", s.leaderMiddleware(s.handleLeaseRevoke))
	mux.HandleFunc("POST "+transport.UriLease+"/keep-alive", s.leaderMiddleware(s.handleLeaseKeepAlive))
	mux.HandleFunc("POST "+transport.UriLease+"/lookup", s.leaderMiddleware(s.handleLeaseLookup))
	// cluster
	mux.HandleFunc("POST "+transport.UriCluster+"/join", s.leaderMiddleware(s.handleJoin))

	mux.HandleFunc("GET /stats", s.handleStats)                              // stats
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{})) // prometheus metrics
	mux.HandleFunc("GET /livez", s.handleLivez)                              // k8s /livez
	mux.HandleFunc("GET /readyz", s.handleReadyz)                            // k8s /readyz

	return s
}

// leaderMiddleware is a wrapper around normal requests that requires a leader,
// internally it checks if we are the leader, and if not we proxy the request to the leader.
//
// If the leader is not found, we return 503
func (s *HttpServer) leaderMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		leader, err := s.peerSvc.GetLeader()
		if err != nil {
			s.logger.Error("LeaderMiddleware error: failed to determine leader", "error", err)
			errMsg := fmt.Sprintf("failed to determine leader: %v", err)
			writeError(w, errMsg, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.peerSvc.Me().NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		next(w, r)
	}
}

func (s *HttpServer) proxyToLeader(w http.ResponseWriter, r *http.Request, leader config.Peer) {
	target := &url.URL{
		Scheme: "http",
		Host:   leader.GetHttpAddress(),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		s.logger.Error("proxy to leader failed",
			"leader_id", leader.NodeID,
			"leader_addr", leader.GetHttpAddress(),
			"error", err,
		)
		errMsg := fmt.Sprintf("failed to proxy request to leader %s: %v", leader.NodeID, err)
		writeError(w, errMsg, http.StatusBadGateway)
	}

	s.logger.Debug("proxying request to leader",
		"leader_id", leader.NodeID,
		"path", r.URL.Path,
	)
	proxy.ServeHTTP(w, r)
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

	var req transport.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error("Failed to decode request body", "error", err)
		response := map[string]string{"error": fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)}
		s.writeJSON(w, response, http.StatusBadRequest)
		return
	}

	err := s.clusterSvc.AddToCluster(r.Context(), req)
	if err != nil {
		s.logger.Error(addClusterErrMsg, "error", err)
		response := map[string]string{"error": fmt.Sprintf("%s: %v", addClusterErrMsg, err)}
		s.writeJSON(w, response, http.StatusInternalServerError)
		return
	}

	s.logger.Info("Peer added to cluster successfully")
	response := map[string]string{"message": "Peer added to cluster successfully"}
	s.writeJSON(w, response, http.StatusOK)
}

func (s *HttpServer) handleLeaseGrant(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_GRANT request")

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
		writeError(w, fmt.Sprintf("%s: %v", leaseGrantErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseGrant: result}, http.StatusOK)
}
func (s *HttpServer) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_REVOKE request")

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
		writeError(w, fmt.Sprintf("%s: %v", leaseRevokeErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseRevoke: result}, http.StatusOK)
}

func (s *HttpServer) handleLeaseKeepAlive(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_KEEP_ALIVE request")

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
		writeError(w, fmt.Sprintf("%s: %v", leaseKeepAliveErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseKeepAlive: result}, http.StatusOK)
}

func (s *HttpServer) handleLeaseLookup(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_LOOKUP request")

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
		writeError(w, fmt.Sprintf("%s: %v", leaseLookupErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, command.Result{LeaseLookup: result}, http.StatusOK)
}

func (s *HttpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received STATS request")
	stats, err := s.clusterSvc.Stats()
	if err != nil {
		s.logger.Error("Failed to get stats", "error", err)
		writeError(w, fmt.Sprintf("failed to get stats: %v", err), http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, stats, http.StatusOK)
}

func (s *HttpServer) handleLivez(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received READYZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.logger.Error("Live check failed", "error", "raft is shutdown")
		s.writeJSON(w, map[string]string{"status": "raft_shutdown"}, http.StatusServiceUnavailable)
		return
	}

	if err := s.kvSvc.Ping(); err != nil {
		s.logger.Error("Live check failed", "error", err)
		statusErr := fmt.Sprintf("kv store ping failed: %v", err)
		s.writeJSON(w, map[string]string{"status": statusErr}, http.StatusServiceUnavailable)
		return
	}

	s.writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

func (s *HttpServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received LIVEZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.logger.Error("Readiness check failed", "error", "raft is shutdown")
		s.writeJSON(w, map[string]string{"status": "raft_shutdown"}, http.StatusServiceUnavailable)
		return
	}

	_, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		statusErr := fmt.Sprintf("failed to get leader info: %v", err)
		s.writeJSON(w, map[string]string{"status": statusErr}, http.StatusServiceUnavailable)
		return
	}

	if err := s.peerSvc.LaggingBehind(); err != nil {
		s.logger.Error("Readiness check failed", "error", err)
		statusErr := fmt.Sprintf("raft is lagging behind: %v", err)
		s.writeJSON(w, map[string]string{"status": statusErr}, http.StatusServiceUnavailable)
		return
	}

	s.writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
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
