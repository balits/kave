package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/transport"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	errRaftShutdown = errors.New("raft is shutdown")
)

const (
	RouteKvRange  string = transport.RouteKv + "/range"
	RouteKvPut    string = transport.RouteKv + "/put"
	RouteKvDelete string = transport.RouteKv + "/delete"
	RouteKvTxn    string = transport.RouteKv + "/txn"

	RouteLeaseGrant     string = transport.RouteLease + "/grant"
	RouteLeaseRevoke    string = transport.RouteLease + "/revoke"
	RouteLeaseKeepAlive string = transport.RouteLease + "/keep-alive"
	RouteLeaseLookup    string = transport.RouteLease + "/lookup"

	RouteOtInit     string = transport.RouteOt + "/init"
	RouteOtTransfer string = transport.RouteOt + "/transfer"
	RouteOtWriteAll string = transport.RouteOt + "/write-all"

	RouteClusterJoin string = transport.RouteCluster + "/join"

	RouteStats   string = "/stats"
	RouteMetrics string = "/metrics"
	RouteLivez   string = "/livez"
	RouteReadyz  string = "/readyz"
)

const (
	kvRangeErrMsg  string = "failed to range over key-value pair(s)"
	kvPutErrMsg    string = "failed to put key-value pair"
	kvDeleteErrMsg string = "failed to delete key-value pair"
	kvTxnErrMsg    string = "failed to execute key-value transaction"

	leaseGrantErrMsg     string = "failed to grant lease"
	leaseRevokeErrMsg    string = "failed to revoke lease"
	leaseKeepAliveErrMsg string = "failed to keep lease alive"
	leaseLookupErrMsg    string = "failed to lookup lease"

	otInitErrMsg     string = "failed to execute OT init"
	otTransferErrMsg string = "failed to execute OT transfer"
	otWriteAllErrMsg string = "failed to execute OT write all"

	clusterJoinErrMsg string = "failed to add peer to the cluster"

	jsonEncodeErrMsg string = "failed to encode JSON body"
	jsonDecodeErrMsg string = "failed to decode JSON body"

	readyzErrMsg string = "readyz check failed"
	statsErrMsg  string = "stats check failed"
	livezErrMsg  string = "livez check failed"
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

	// kv
	mux.HandleFunc("GET "+RouteKvRange, s.readMiddleware(s.handleKvRange)) // optional leader if we want the latest data
	mux.HandleFunc("POST "+RouteKvPut, s.writeMiddleware(s.handleKvPut))
	mux.HandleFunc("DELETE "+RouteKvDelete, s.writeMiddleware(s.handleKvDelete))
	mux.HandleFunc("POST "+RouteKvTxn, s.writeMiddleware(s.handleKvTxn))

	// lease
	mux.HandleFunc("POST "+RouteLeaseGrant, s.writeMiddleware(s.handleLeaseGrant))
	mux.HandleFunc("DELETE "+RouteLeaseRevoke, s.writeMiddleware(s.handleLeaseRevoke))
	mux.HandleFunc("POST "+RouteLeaseKeepAlive, s.writeMiddleware(s.handleLeaseKeepAlive))
	mux.HandleFunc("GET "+RouteLeaseLookup, s.readMiddleware(s.handleLeaseLookup)) // optional leader if we want the most "up to date" data regarding a lease's ttl

	// ot
	mux.HandleFunc("GET "+RouteOtInit, s.handleOTInit)                             // Init does not read anything from the backend, no middleware need
	mux.HandleFunc("GET "+RouteOtTransfer, s.readMiddleware(s.handleOTTransfer))   // optional leader if we want the latest data
	mux.HandleFunc("POST "+RouteOtWriteAll, s.writeMiddleware(s.handleOTWriteAll)) // write must go through raft

	// cluster
	mux.HandleFunc("POST "+RouteClusterJoin, s.writeMiddleware(s.handleJoin))

	// health / debug
	mux.HandleFunc("GET "+RouteStats, s.handleStats)                                  // stats
	mux.Handle("GET "+RouteMetrics, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})) // prometheus metrics
	mux.HandleFunc("GET "+RouteLivez, s.handleLivez)                                  // k8s /livez
	mux.HandleFunc("GET "+RouteReadyz, s.handleReadyz)                                // k8s /readyz

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

func (s *HttpServer) handleKvRange(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_GET request")

	var req api.RangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Range(r.Context(), req)
	if err != nil {
		s.logger.Error(kvRangeErrMsg, "error", err)
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvRangeErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKvPut(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_PUT request")

	var req api.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Put(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvPutErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKvDelete(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_DELETE request")

	var req api.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Delete(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvDeleteErrMsg, err, status)
		return
	}

	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleKvTxn(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received KV_TXN request")

	var req api.TxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.kvSvc.Txn(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, kvTxnErrMsg, err, status)
		return
	}

	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received CLUSTER_JOIN request")

	var req transport.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	err := s.clusterSvc.AddToCluster(r.Context(), req)
	if err != nil {
		s.writeError(w, clusterJoinErrMsg, err, http.StatusInternalServerError)
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
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.Grant(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseGrantErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}
func (s *HttpServer) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_REVOKE request")

	var req api.LeaseRevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.Revoke(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseRevokeErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleLeaseKeepAlive(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_KEEP_ALIVE request")

	var req api.LeaseKeepAliveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.KeepAlive(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseKeepAliveErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleLeaseLookup(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("received LEASE_LOOKUP request")

	var req api.LeaseLookupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.leaseSvc.Lookup(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, leaseLookupErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleOTInit(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Recieved OT_INIT request")

	// req is empty by desing as of yet, still
	// an empty body is the valid one,
	// we dont want to get sent random json
	var req api.OTInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.otSvc.Init(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, otInitErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleOTTransfer(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Recieved OT_TRANSFER request")

	var req api.OTTransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	reponse, err := s.otSvc.Transfer(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, otTransferErrMsg, err, status)
		return
	}
	s.writeJSON(w, reponse, status)
}

func (s *HttpServer) handleOTWriteAll(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Recieved OT_WRITE_ALL request")

	var req api.OTWriteAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, jsonDecodeErrMsg, err, http.StatusBadRequest)
		return
	}

	status := http.StatusOK
	response, err := s.otSvc.WriteAll(r.Context(), req)
	if err != nil {
		if errors.Is(err, util.ProposeError) {
			status = http.StatusServiceUnavailable
		} else {
			status = http.StatusBadRequest
		}
		s.writeError(w, otWriteAllErrMsg, err, status)
		return
	}
	s.writeJSON(w, response, status)
}

func (s *HttpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received STATS request")
	stats, err := s.clusterSvc.Stats()
	if err != nil {
		s.writeError(w, statsErrMsg, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, stats, http.StatusOK)
}

func (s *HttpServer) handleLivez(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received READYZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.writeError(w, livezErrMsg, errRaftShutdown, http.StatusServiceUnavailable)
		return
	}

	if err := s.kvSvc.Ping(); err != nil {
		s.writeError(w, "kv store ping failed", err, http.StatusServiceUnavailable)
		return
	}

	s.writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

func (s *HttpServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Received LIVEZ request")
	if s.peerSvc.State() == raft.Shutdown {
		s.writeError(w, readyzErrMsg, errRaftShutdown, http.StatusServiceUnavailable)
		return
	}

	_, err := s.peerSvc.GetLeader()
	if err != nil {
		msg := readyzErrMsg + ": failed to get leader info"
		s.writeError(w, msg, err, http.StatusServiceUnavailable)
		return
	}

	if err := s.peerSvc.LaggingBehind(); err != nil {
		msg := readyzErrMsg + ": raft is lagging behind"
		s.writeError(w, msg, err, http.StatusServiceUnavailable)
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

// writeError logs the message with the provided error then composes them
// into a single string in the form "msg: error" and writes it as a JSON response
// with the provided status code. The optional attrs will be provided to the logger as additional context
func (s *HttpServer) writeError(w http.ResponseWriter, msg string, err error, status int, attrs ...any) {
	attrs = append(attrs, "error", err.Error())
	s.logger.Error(msg, attrs...)
	jsonMessage := fmt.Sprintf("%s: %v", msg, err)
	w.Header().Set("Content-Type", "application/json")
	bytes, _ := json.Marshal(map[string]string{"error": jsonMessage})
	w.WriteHeader(status)
	w.Write(bytes)
}
