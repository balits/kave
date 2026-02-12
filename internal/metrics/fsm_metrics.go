package metrics

import (
	"sync/atomic"
	"time"
)

type FsmMetricsProvider interface {
	FsmMetrics() *FsmMetrics
}

type FsmMetrics struct {
	Term          uint64
	ApplyIndex    uint64
	LastApplyTime time.Time
}

type FsmMetricsAtomic struct {
	Term               atomic.Uint64
	ApplyIndex         atomic.Uint64
	LastApplyTimeNanos atomic.Int64
}

func (a *FsmMetricsAtomic) FsmMetrics() *FsmMetrics {
	return &FsmMetrics{
		Term:          a.Term.Load(),
		ApplyIndex:    a.ApplyIndex.Load(),
		LastApplyTime: atomicNanosToTime(&a.LastApplyTimeNanos),
	}
}

func atomicNanosToTime(n *atomic.Int64) time.Time {
	inner := n.Load()
	if inner == 0 {
		return time.Time{}
	}
	return time.Unix(0, inner)
}
