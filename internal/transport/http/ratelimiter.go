package http

import (
	"errors"
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
func (rl *rateLimiter) Limiter(path string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if l, ok := rl.limiters[path]; ok {
		return l
	}
	l := rate.NewLimiter(rate.Limit(rl.cfg.rps), int(rl.cfg.burst))
	rl.limiters[path] = l
	return l
}
