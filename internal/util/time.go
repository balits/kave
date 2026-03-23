package util

import (
	"context"
	"time"
)

type Clock interface {
	Now() time.Time
	Until(time.Time) time.Duration
}

type RealClock struct{}

func NewRealClock() Clock {
	return &RealClock{}
}

func (rc *RealClock) Now() time.Time                  { return time.Now() }
func (rc *RealClock) Until(t time.Time) time.Duration { return time.Until(t) }

type FakeClock struct {
	now time.Time
}

func NewFakeClock(start time.Time) Clock {
	return &FakeClock{
		now: start,
	}
}

// AdvanceSeconds előre tekeri az időt deltaS másodperccel
func (fc *FakeClock) AdvanceSeconds(deltaS time.Duration) { fc.now = fc.now.Add(deltaS * time.Second) }
func (fc *FakeClock) Override(t time.Time)                { fc.now = t }
func (fc *FakeClock) Now() time.Time                      { return fc.now }
func (fc *FakeClock) Until(t time.Time) time.Duration {
	return t.Sub(fc.now)
}

type Ticker interface {
	Tick() <-chan time.Time
}

type RealTicker struct {
	c <-chan time.Time
}

func NewRealTicker(d time.Duration) Ticker {
	return &RealTicker{
		c: time.Tick(d),
	}
}

func (rt *RealTicker) Tick() <-chan time.Time { return rt.c }

type FakeTicker struct {
	c chan time.Time
}

func NewFakeTicker() Ticker {
	return &FakeTicker{
		c: make(chan time.Time),
	}
}

func (ft *FakeTicker) FakeTick() {
	if ft.c != nil {
		ft.c <- time.Now()
	}
}

func (ft *FakeTicker) TickOrDone(ctx context.Context) {
	select {
	case ft.c <- time.Now():
	case <-ctx.Done():
	}
}

func (ft *FakeTicker) Tick() <-chan time.Time { return ft.c }
func (ft *FakeTicker) Stop()                  { close(ft.c) }
