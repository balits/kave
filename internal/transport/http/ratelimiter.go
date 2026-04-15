package http

import (
	"errors"
	"sync"

	"golang.org/x/time/rate"
)

type RatelimitOpions struct {
	Read  ratelimiterConfig `json:"read"`
	Write ratelimiterConfig `json:"write"`
}

var errMsgRateLimiter = errors.New("rate limiter error")

type ratelimiterConfig struct {
	Rps   uint `json:"rsp"`
	Burst uint `json:"burst"`
}

func NewRateLimiterConfig(r, b uint) ratelimiterConfig {
	return ratelimiterConfig{
		Rps:   r,
		Burst: b,
	}
}

type rateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	cfg      ratelimiterConfig
}

func newRateLimiter(cfg ratelimiterConfig) *rateLimiter {
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
	l := rate.NewLimiter(rate.Limit(rl.cfg.Rps), int(rl.cfg.Burst))
	rl.limiters[path] = l
	return l
}
