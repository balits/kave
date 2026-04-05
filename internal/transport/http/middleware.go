package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/balits/kave/internal/config"
)

const (
	errMsgReadMiddleware  string = "read middleware error"
	errMsgWriteMiddleware string = "write middleware error"
	errMsgProxyLeader     string = "proxying to leader failed"
)


func (s *HttpServer) readMiddleware(next http.HandlerFunc) http.HandlerFunc {
	limiter := s.readLimiter.Middleware(s, next)

	return func(w http.ResponseWriter, r *http.Request) {
		drainedBytes, err := drainBody(r.Body)
		if err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(drainedBytes))

		var peek struct {
			Serializable bool `json:"serializable"`
		}

		if err := json.Unmarshal(drainedBytes, &peek); err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusBadRequest)
			return
		}

		if peek.Serializable {
			limiter(w, r)
			return
		}

		leader, err := s.peerSvc.GetLeader()
		if err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.peerSvc.Me().NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		if err := s.peerSvc.VerifyLeader(r.Context()); err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		limiter(w, r)
	}
}

func (s *HttpServer) writeMiddleware(next http.HandlerFunc) http.HandlerFunc {
	limiter := s.writeLimiter.Middleware(s, next)

	return func(w http.ResponseWriter, r *http.Request) {
		leader, err := s.peerSvc.GetLeader()
		if err != nil {
			s.writeError(w, errMsgWriteMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.peerSvc.Me().NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		limiter(w, r)
	}
}

func (s *HttpServer) proxyToLeader(w http.ResponseWriter, r *http.Request, leader config.Peer) {
	target := &url.URL{
		Scheme: "http",
		Host:   leader.GetHttpAdvertisedAddress(),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		s.writeError(w, errMsgProxyLeader, err, http.StatusBadGateway,
			"leader_id", leader.NodeID,
			"leader_addr", leader.GetHttpAdvertisedAddress(),
		)
	}
	limiter := s.writeLimiter.Middleware(s, proxy.ServeHTTP)

	s.logger.Debug("proxying request to leader",
		"leader_id", leader.NodeID,
		"path", r.URL.Path,
	)
	limiter(w, r)
}

func drainBody(oldBody io.ReadCloser) (read []byte, err error) {
	defer oldBody.Close()
	read, err = io.ReadAll(oldBody)
	if err != nil {
		return nil, fmt.Errorf("draining body failed: %w", err)
	}
	return
}

// just put it here for later, when we add TLS
func corsMiddleware(allowedOrigin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
