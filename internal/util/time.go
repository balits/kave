package util

import "time"

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
	C() <-chan time.Time
	Stop()
}

type RealTicker struct {
	ticker time.Ticker
}

func NewRealTicker(d time.Duration) Ticker {
	return &RealTicker{
		ticker: *time.NewTicker(d),
	}
}

func (rt *RealTicker) C() <-chan time.Time { return rt.ticker.C }
func (rt *RealTicker) Stop()               { rt.ticker.Stop() }

type FakeTicker struct {
	c chan time.Time
}

func NewFakeTicker() Ticker {
	return &FakeTicker{
		c: make(chan time.Time),
	}
}

func (ft *FakeTicker) Tick() {
	if ft.c != nil {
		ft.c <- time.Now()
	}
}

func (ft *FakeTicker) C() <-chan time.Time { return ft.c }
func (ft *FakeTicker) Stop()               { close(ft.c) }
