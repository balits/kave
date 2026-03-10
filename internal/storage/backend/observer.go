package backend

import (
	"time"

	"github.com/balits/kave/internal/metrics"
)

// BackendObserver egy OnCommit hook-on keresztük tudja frissíteni a backend
// metrikák committal kapcsolatos részét
type BackendObserver interface {
	ObserveCommit(time time.Duration, batchSize int)
}

type observer struct {
	metrics *metrics.BackendMetrics
}

func (o *observer) ObserveCommit(duration time.Duration, batchSize int) {
	o.metrics.CommitDurationSec.Observe(duration.Seconds())
	o.metrics.BatchSize.Observe(float64(batchSize))
}
