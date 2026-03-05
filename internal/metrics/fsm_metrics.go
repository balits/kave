package metrics

import (
	"sync/atomic"
	"time"
)

type FsmMetricsProvider interface {
	FsmMetrics() *FsmMetrics
}

type FsmMetrics struct {
	Term       uint64
	ApplyIndex uint64
	//LastApplyTime time.Time
}

func (m *FsmMetrics) FsmMetrics() *FsmMetrics {
	return &FsmMetrics{
		Term:       atomic.LoadUint64(&m.Term),
		ApplyIndex: atomic.LoadUint64(&m.ApplyIndex),
	}
}

func atomicNanosToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
