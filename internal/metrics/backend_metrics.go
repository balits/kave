package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type BackendMetrics struct {
	SizeBytes prometheus.GaugeFunc // derived from

	CommitDurationSec prometheus.Histogram
	BatchSize         prometheus.Histogram // number of operations in a batch -> is batching even efficient?
}

func NewBackendMetrics(
	reg prometheus.Registerer,
	sizeBytesFn gaugeFn,
) *BackendMetrics {
	factory := promauto.With(reg)

	return &BackendMetrics{
		SizeBytes: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "backend",
			Name:      "size_bytes",
			Help:      "Current size of the storage in bytes.",
		}, sizeBytesFn),
		CommitDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "backend",
			Name:      "commit_duration_seconds",
			Help:      "Latency of commit operations.",
			Buckets:   []float64{.001, .005, .01, .05, .1, .5, 1},
		}),
		BatchSize: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "backend",
			Name:      "batch_size",
			Help:      "Number of operations in a batch.",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 500, 1000},
		}),
	}
}
