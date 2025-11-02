package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/web"
)

var registerOnce sync.Once

type Server struct {
	// http.Server is embedded into api.Server
	http.Server

	// Logger provides structured logging
	Logger *slog.Logger

	// node is a reference to the nodes state, giving acces to its Raft and Store instance (needed for api calls)
	node *raftnode.Node

	// mux is the http.ServeMux thats used to register routes, and it gets plugged into http.Server.Handler
	mux *http.ServeMux
}

func NewServer(addr string, node *raftnode.Node, logger *slog.Logger) *Server {
	return &Server{
		Server: http.Server{
			Addr:         addr,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 5 * time.Second,
			IdleTimeout:  10 * time.Second,
		},
		node:   node,
		Logger: logger,
		mux:    nil,
	}
}

func (s *Server) Register(pattern string, handler func(*web.Context)) {
	if s.mux == nil {
		return
	}
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler(web.NewContext(w, r))
		duration := time.Since(start)
		method := r.Method
		path := r.URL.Path
		s.Logger.Info("Incoming request", "method", method, "path", path, "duration", duration)
	})
}

func (s *Server) RegisterRoutes() {
	registerOnce.Do(func() {
		if s.mux != nil {
			return
		}
		s.mux = http.DefaultServeMux
		s.Register("GET /get", s.getHandler)
		s.Register("POST /set", s.setHandler)
		s.Register("DELETE /delete", s.deleteHandler)
		s.Register("POST /join", s.joinHandler)
		// s.Register("GET /health", s.healthHandler)
	})
}

// Run starts the http server and handles its shutdown
func (s *Server) Run() {
	s.Logger.Info("Started HTTP servers", "address", s.Addr)
	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.Logger.Error("Failed to start HTTP server", "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		s.Logger.Error("Failed to shutdown HTTP server", "error", err)
	}
	s.Logger.Info("Shutdown HTTP server")
}
