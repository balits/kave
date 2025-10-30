// web contains the http server implementation
package web

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type Server struct {
	server *http.Server
	Router Router
}

// NewServer creates a router from scratch and returns a new server with that router and the supplied Logger
// Note that the logger will not be added any additional prefixes through this function, the caller should specify them
func NewServer(addr string, logger *slog.Logger) *Server {
	router := NewRouterWithLogger(logger)
	s := &http.Server{
		Addr:         addr,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  10 * time.Second,
		Handler:      WithLogger(router, logger),
	}
	return &Server{
		server: s,
		Router: router,
	}
}

func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.server.Handler.ServeHTTP(w, r)
}

// Start starts the http server, returning an error if any
func (srv *Server) Start() error {
	if err := srv.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (srv *Server) Addr() string {
	return srv.server.Addr
}

// Shutdown terminates the http server with the supplied timeout
func (srv *Server) Shutdown(timeout time.Duration) error {
	c, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := srv.server.Shutdown(c)
	return err
}

type HandlerFunc func(ctx *Context)

type route struct {
	method  string
	pattern string
}

type Router struct {
	routes map[route]HandlerFunc
	logger *slog.Logger
}

func NewRouter() Router {
	return Router{
		routes: make(map[route]HandlerFunc),
	}
}

func NewRouterWithLogger(logger *slog.Logger) Router {
	return Router{
		routes: make(map[route]HandlerFunc),
		logger: logger,
	}
}

func (r *Router) Register(method string, pattern string, handler HandlerFunc) {
	route := route{method: method, pattern: pattern}
	r.routes[route] = handler
}

func (r Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pattern := req.URL.Path
	method := req.Method

	h, ok := r.routes[route{method: method, pattern: pattern}]
	ctx := NewContext(w, req)
	if !ok {
		ctx.NotFound()
		return
	} else if method != req.Method {
		ctx.MethodNotAllowed()
		return
	}
	h(ctx)
}

// WithLogger provides a logging middleware by wrapping a [http.Handler] with logging information about the request
func WithLogger(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		method := r.Method
		path := r.URL.Path
		logger.Info("New request", "method", method, "path", path, "duration", duration.String())
	})
}
