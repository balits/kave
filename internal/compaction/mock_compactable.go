package compaction

import (
	"sync"
	"time"

	"github.com/balits/kave/internal/kv"
)

type mockCompactable struct {
	mu           sync.Mutex
	rev          int64
	compactedRev int64
	delay        time.Duration
	compactCalls []int64
	err          error
}

func newMockCompactable(startRev, compacted int64, delay time.Duration) *mockCompactable {
	return &mockCompactable{
		rev:          startRev,
		compactedRev: compacted,
		delay:        delay,
	}
}

func (mc *mockCompactable) Revisions() (kv.Revision, int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return kv.Revision{Main: mc.rev}, mc.compactedRev
}

func (mc *mockCompactable) Compact(targetRev int64) (<-chan struct{}, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.err != nil {
		return nil, mc.err
	}

	mc.compactedRev = targetRev
	mc.compactCalls = append(mc.compactCalls, targetRev)

	doneC := make(chan struct{})
	go func() {
		if mc.delay > 0 {
			time.Sleep(mc.delay)
		}
		close(doneC)
	}()

	return doneC, nil
}

func (mc *mockCompactable) getCalls() []int64 {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.compactCalls
}

func (mc *mockCompactable) setRev(r int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if r < mc.rev {
		panic("attempted to set revision to an lesser number")
	}
	mc.rev = r
}
