package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/web"
)

type Server struct {
	http.Server
	Logger *slog.Logger
	node   *raftnode.Node
	mux    *http.ServeMux
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
		handler(web.NewContext(w, r))
	})
}

func (s *Server) RegisterRoutes() {
	if s.mux != nil {
		return
	}
	s.mux = http.DefaultServeMux
	s.Register("GET /get", s.getHandler)
	s.Register("POST /set", s.setHandler)
	s.Register("DELETE /delete", s.deleteHandler)
	s.Register("POST /join", s.joinHandler)
}

// Start starts the http server, returning an error if any
func (s *Server) Start() error {

	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Addr() string {
	return s.Addr()
}

// Shutdown terminates the http server with the supplied timeout
func (s *Server) Shutdown(timeout time.Duration) error {
	if err := s.Shutdown(timeout); err != nil {
		return err
	}
	return nil
}
