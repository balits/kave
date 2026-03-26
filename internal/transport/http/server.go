package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	otInitErrMsg     string = "failed to execute OT init"
	otTransferErrMsg string = "failed to execute OT transfer"
	otWriteAllErrMsg string = "failed to execute OT write all"

	addClusterErrMsg string = "failed to add peer to the cluster"

	jsonEncodeErrMsg string = "failed to encode JSON body"
	jsonDecodeErrMsg string = "failed to decode JSON body"
)

var (
	errConsistencyMiddleware = errors.New("consistency middleware error")
	errLeaderMiddleware      = errors.New("leader middleware error")
	errProxyLeader           = errors.New("proxying to leader failed")
	errLeaderCheck           = errors.New("failed to determine leader")
)

type HttpServer struct {
	kvSvc      service.KVService
	leaseSvc   service.LeaseService
	otSvc      service.OTService
	clusterSvc service.ClusterService
	peerSvc    service.PeerService
	logger     *slog.Logger
	server     *http.Server
}

func NewHTTPServer(
	logger *slog.Logger,
	addr string,
	kvService service.KVService,
	leaseService service.LeaseService,
	otService service.OTService,
	clusterService service.ClusterService,
	peerService service.PeerService,
	reg *prometheus.Registry,
) *HttpServer {
	mux := http.NewServeMux()
	s := &HttpServer{
		kvSvc:      kvService,
		leaseSvc:   leaseService,
		otSvc:      otService,
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
	mux.HandleFunc("GET "+transport.UriKv+"/get", s.consistencyModeMiddleware(s.handleKvGet)) // optional leader middleware if we want the latest data
	mux.HandleFunc("POST "+transport.UriKv+"/put", s.leaderMiddleware(s.handleKvPut))
	mux.HandleFunc("DELETE "+transport.UriKv+"/delete", s.leaderMiddleware(s.handleKvDelete))
	mux.HandleFunc("POST "+transport.UriKv+"/txn", s.leaderMiddleware(s.handleKvTxn))
	// lease
	mux.HandleFunc("POST "+transport.UriLease+"/grant", s.leaderMiddleware(s.handleLeaseGrant))
	mux.HandleFunc("DELETE "+transport.UriLease+"/revoke", s.leaderMiddleware(s.handleLeaseRevoke))
	mux.HandleFunc("POST "+transport.UriLease+"/keep-alive", s.leaderMiddleware(s.handleLeaseKeepAlive))
	mux.HandleFunc("POST "+transport.UriLease+"/lookup", s.leaderMiddleware(s.handleLeaseLookup))
	// ot
	mux.HandleFunc("GET "+transport.UriOT+"/init", s.handleOTInit)                                      // Init does not read anything from the backend, no middleware need
	mux.HandleFunc("GET "+transport.UriOT+"/transfer", s.consistencyModeMiddleware(s.handleOTTransfer)) // optional leader middleware if we want the latest data
	mux.HandleFunc("POST "+transport.UriOT+"/write-all", s.leaderMiddleware(s.handleOTWriteAll))
	// cluster
	mux.HandleFunc("POST "+transport.UriCluster+"/join", s.leaderMiddleware(s.handleJoin))

	mux.HandleFunc("GET /stats", s.handleStats)                              // stats
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{})) // prometheus metrics
	mux.HandleFunc("GET /livez", s.handleLivez)                              // k8s /livez
	mux.HandleFunc("GET /readyz", s.handleReadyz)                            // k8s /readyz

	return s
}

// leaderMiddleware is a wrapper around normal requests that requires a leader.
// Internally it checks if we are the leader, and if not we proxy the request to the leader.
// If the leader is not found, we return 503
func (s *HttpServer) leaderMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		leader, err := s.peerSvc.GetLeader()
		if err != nil {
			msg := fmt.Sprintf("%s: %s", errLeaderMiddleware, errLeaderCheck)
			s.logger.Error(msg, "error", err)
			writeError(w, fmt.Sprintf("%s: %s", errLeaderCheck, err), http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.peerSvc.Me().NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		next(w, r)
	}
}

// consistencyModeMiddleware is a wrapper around normal requests whose response body
// contains a "serializable" field, used to determine if we need to proxy to the leader or not.
//
// Internally it peeks at and replays the request body to find the "serializable" field,
// then if its false we pass the request to [leaderMiddleware].
//
// If the reading the body was unsuccessful, we return 400
func (s *HttpServer) consistencyModeMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		newBody, err := drainBody(r.Body)
		if err != nil {
			msg := fmt.Sprintf("%s: failed to read body", errConsistencyMiddleware)
			s.logger.Error(msg, "error", err)
			writeError(w, fmt.Sprintf("%v: %v", msg, err), http.StatusBadRequest)
			return
		}
		r.Body = newBody

		var peek struct {
			Serializable bool `json:"serializable"`
		}
		if err := json.NewDecoder(newBody).Decode(&peek); err != nil {
			msg := fmt.Sprintf("%s: failed to decode body", errConsistencyMiddleware)
			s.logger.Error(msg, "error", err)
			writeError(w, fmt.Sprintf("%v: %v", msg, err), http.StatusBadRequest)
			return
		}

		if !peek.Serializable {
			s.leaderMiddleware(next)
			return
		}

		next(w, r)
	}
}

func (s *HttpServer) proxyToLeader(w http.ResponseWriter, r *http.Request, leader config.Peer) {
	target := &url.URL{
		Scheme: "http",
		Host:   leader.GetHttpAdvertisedAddress(),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		s.logger.Error("proxy to leader failed",
			"leader_id", leader.NodeID,
			"leader_addr", leader.GetHttpAdvertisedAddress(),
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

	if !req.Serializable {
		leader, err := s.peerSvc.GetLeader()
		if err != nil {
			s.logger.Error("failed to determine leader", "error", err)
			errMsg := fmt.Sprintf("failed to determine leader: %v", err)
			writeError(w, errMsg, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.peerSvc.Me().NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}
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

func (s *HttpServer) handleOTInit(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Recieved OT_INIT request")

	var req api.OTInitRequest
	// req is empty by desing as of yet
	// if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
	// 	s.logger.Error(jsonDecodeErrMsg, "error", err)
	// 	err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
	// 	writeError(w, err, http.StatusBadRequest)
	// 	return
	// }

	result, err := s.otSvc.Init(r.Context(), req)
	if err != nil {
		s.logger.Error(otInitErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", otInitErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, result, http.StatusOK)
}

func (s *HttpServer) handleOTTransfer(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Recieved OT_TRANSFER request")

	var req api.OTTransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.otSvc.Transfer(r.Context(), req)
	if err != nil {
		s.logger.Error(otTransferErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", otTransferErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, result, http.StatusOK)
}

func (s *HttpServer) handleOTWriteAll(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Recieved OT_WRITE_ALL request")

	var req api.OTWriteAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error(jsonDecodeErrMsg, "error", err)
		err := fmt.Sprintf("%s: %v", jsonDecodeErrMsg, err)
		writeError(w, err, http.StatusBadRequest)
		return
	}

	result, err := s.otSvc.WriteAll(r.Context(), req)
	if err != nil {
		s.logger.Error(otWriteAllErrMsg, "error", err)
		writeError(w, fmt.Sprintf("%s: %v", otWriteAllErrMsg, err), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, result, http.StatusOK)
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
		writeError(w, "raft_shutdown", http.StatusServiceUnavailable)
		return
	}

	if err := s.kvSvc.Ping(); err != nil {
		s.logger.Error("Live check failed", "error", err)
		writeError(w, fmt.Sprintf("kv store ping failed: %v", err), http.StatusServiceUnavailable)
		return
	}

	s.writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

func (s *HttpServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received LIVEZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.logger.Error("Readiness check failed", "error", "raft is shutdown")
		writeError(w, "raft_shutdown", http.StatusServiceUnavailable)
		return
	}

	_, err := s.peerSvc.GetLeader()
	if err != nil {
		s.logger.Error("Failed to get leader info", "error", err)
		writeError(w, fmt.Sprintf("failed to get leader info: %v", err), http.StatusServiceUnavailable)
		return
	}

	if err := s.peerSvc.LaggingBehind(); err != nil {
		s.logger.Error("Readiness check failed", "error", err)
		writeError(w, fmt.Sprintf("raft is lagging behind: %v", err), http.StatusServiceUnavailable)
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

func drainBody(oldBody io.ReadCloser) (newBody io.ReadCloser, err error) {
	read, err := io.ReadAll(oldBody)
	defer oldBody.Close()
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(read)), nil
}
