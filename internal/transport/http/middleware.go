package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/balits/kave/internal/peer"
)

const (
	errMsgReadMiddleware  string = "read middleware error"
	errMsgWriteMiddleware string = "write middleware error"
	errMsgProxyLeader     string = "proxying to leader failed"
)

type middleware = func(http.HandlerFunc) http.HandlerFunc

func chain(base http.HandlerFunc, ms ...middleware) http.HandlerFunc {
	for i := range len(ms) {
		base = ms[len(ms)-i-1](base)
	}
	return base
}

// func (s *HttpServer) statusChain(base http.HandlerFunc) http.HandlerFunc {
// 	return chain(base, s.requestLoggingMiddleware)
// }

func (s *HttpServer) readChain(base http.HandlerFunc) http.HandlerFunc {
	return chain(base, s.requestLoggingMiddleware, s.readLimitMiddleware, s.consitencyMiddleware)
}

func (s *HttpServer) consitencyMiddleware(next http.HandlerFunc) http.HandlerFunc {
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
			next(w, r)
			return
		}

		leader, err := s.raftSvc.Leader()
		if err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.me.NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		if err := s.raftSvc.VerifyLeader(r.Context()); err != nil {
			s.writeError(w, errMsgReadMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		next(w, r)
	}
}

func drainBody(oldBody io.ReadCloser) (read []byte, err error) {
	defer func() {
		_ = oldBody.Close()
	}()

	read, err = io.ReadAll(oldBody)
	if err != nil {
		return nil, fmt.Errorf("draining body failed: %w", err)
	}
	return
}

func (s *HttpServer) writeChain(base http.HandlerFunc) http.HandlerFunc {
	return chain(base, s.requestLoggingMiddleware, s.writeLimitMiddleware, s.leaderMiddleware)
}

func (s *HttpServer) leaderMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		leader, err := s.raftSvc.Leader()
		if err != nil {
			s.writeError(w, errMsgWriteMiddleware, err, http.StatusServiceUnavailable)
			return
		}

		if leader.NodeID != s.me.NodeID {
			s.proxyToLeader(w, r, leader)
			return
		}

		next(w, r)
	}
}

func (s *HttpServer) proxyToLeader(w http.ResponseWriter, r *http.Request, leader peer.Peer) {
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

	s.logger.Debug("proxying request to leader",
		"leader_id", leader.NodeID,
		"path", r.URL.Path,
	)
	proxy.ServeHTTP(w, r)
}

func (s *HttpServer) requestLoggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		s.logger.WithGroup("request").
			Debug("new request",
				"method", r.Method,
				"url", r.URL.Path,
				"start_time", start,
			)

		i := newStatusCodeInterceptor(w)
		next.ServeHTTP(i, r)

		s.logger.WithGroup("request").
			Debug("request finished",
				"method", r.Method,
				"url", r.URL.Path,
				"elapsed_time", time.Since(start),
				"status_code", i.statusCode,
			)
	}
}

type statusCodeInterceptor struct {
	w          http.ResponseWriter
	statusCode int
}

func newStatusCodeInterceptor(w http.ResponseWriter) *statusCodeInterceptor {
	return &statusCodeInterceptor{
		w:          w,
		statusCode: http.StatusOK,
	}
}

func (i *statusCodeInterceptor) Header() http.Header {
	return i.w.Header()
}

func (i *statusCodeInterceptor) Write(bb []byte) (int, error) {
	return i.w.Write(bb)
}

func (i *statusCodeInterceptor) WriteHeader(statusCode int) {
	i.statusCode = statusCode
	i.w.WriteHeader(statusCode)
}
