package testutil

import (
	"testing"
	"time"
)

func AssertEventually(t *testing.T, condition func() bool, timeout, interval time.Duration) {
	var (
		tryConditionCh = make(chan bool)
		checkCondition = func() { tryConditionCh <- condition() }
		timer          = time.NewTimer(timeout)
		ticker         = time.NewTicker(interval)
	)
	defer func() {
		defer timer.Stop()
		defer ticker.Stop()
	}()
	go checkCondition() // initial check so we dont wait for first tick

	// different ticker channel bcs  fist call to checkCondition takes time, during which ticker could tick
	// instead set it to nil before check, and back to ticker.C after failed assertion
	var tickerCh <-chan time.Time
	for {
		select {
		case <-timer.C:
			t.Errorf("Some nodes failed to join the cluster")
		case <-tickerCh:
			tickerCh = nil
			go checkCondition()
		case result := <-tryConditionCh:
			if result {
				t.Log("All nodes joined the cluster")
				return
			} else {
				tickerCh = ticker.C
			}
		}
	}
}
