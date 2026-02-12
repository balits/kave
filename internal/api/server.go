package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/web"
)

type Server struct {
	// http.Server is embedded into api.Server
	http.Server

	// Logger provides structured logging
	Logger *slog.Logger

	// node is a reference to the nodes state, giving acces to its Raft and Store instance (needed for api calls)
	node *raftnode.Node
}

func NewServer(addr string, node *raftnode.Node, logger *slog.Logger) *Server {
	s := &Server{
		Server: http.Server{
			Addr:         addr,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 5 * time.Second,
			IdleTimeout:  10 * time.Second,
		},
		node:   node,
		Logger: logger.With("module", "http_server"),
	}
	s.Handler = s.newMux()
	s.Logger.Debug("http server created", "addr", addr)
	return s
}

func (s *Server) register(mux *http.ServeMux, pattern string, handler func(*web.Context)) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := web.NewContext(w, r)
		handler(ctx)
		duration := time.Since(start)
		method := r.Method
		path := r.URL.Path
		status := ctx.Status
		s.Logger.Info("Incoming request", "method", method, "path", path, "status", status, "duration", duration)
	})
}

func (s *Server) newMux() *http.ServeMux {
	var routes = map[string]func(*web.Context){
		"GET /get":       s.getHandler,
		"POST /set":      s.setHandler,
		"DELETE /delete": s.deleteHandler,
		"POST /join":     s.joinHandler,
		"GET /status":    s.readyHandler, // for human readable debugging
	}

	mux := http.NewServeMux()
	for route, handler := range routes {
		s.register(mux, route, handler)
	}

	return mux
}

// Run starts the http server go routine.
// The server is stopped when the context gets cancelled
// or an error happens inside the server
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.Logger.Info("Started HTTP servers", "address", s.Addr)
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		} else {
			errCh <- nil // no error, just closed
		}
	}()

	select {
	case <-ctx.Done():
		s.Logger.Debug("server context cancelled, shutting down server", "addr", s.Addr)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.SetKeepAlivesEnabled(false)
		if err := s.Shutdown(ctx); err != nil {
			_ = s.Close()
			s.Logger.Error("Failed to shutdown HTTP server", "error", err, "addr", s.Addr)
			return err
		}
		s.Logger.Info("Shutdown HTTP server", "addr", s.Addr)
		return <-errCh // wait till goroutine finishes, return error if any or nil
	case err := <-errCh:
		return err
	}

}
