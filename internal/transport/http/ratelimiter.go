package http

import (
	"errors"
	"net/http"
	"sync"

	"golang.org/x/time/rate"
)

var errMsgRateLimiter = errors.New("rate limiter error")

type RateLimiterConfig struct {
	rps   uint
	burst uint
}

func NewRateLimiterConfig(r, b uint) RateLimiterConfig {
	return RateLimiterConfig{
		rps:   r,
		burst: b,
	}
}

type rateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	cfg      RateLimiterConfig
}

func newRateLimiter(cfg RateLimiterConfig) *rateLimiter {
	return &rateLimiter{
		limiters: make(map[string]*rate.Limiter),
		cfg:      cfg,
	}
}

// Limiter retrieves a rate limitier instance for the given path,
// or creates it if its not already registered.
func (m *rateLimiter) Limiter(path string) *rate.Limiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.limiters[path]; ok {
		return l
	}
	l := rate.NewLimiter(rate.Limit(m.cfg.rps), int(m.cfg.burst))
	m.limiters[path] = l
	return l
}

// Middleware wraps a handler function around the rate limiter,
// returning 429 if the limit is exceeded.
func (m *rateLimiter) Middleware(s *HttpServer, next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := m.Limiter(r.URL.Path)

		if !l.Allow() {
			s.writeError(w, "limit exceeded", errMsgRateLimiter, http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
