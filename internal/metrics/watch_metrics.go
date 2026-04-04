package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type watchinfo interface {
	SyncedWatchesLen() int
	UnsyncedWatchesLen() int
}

type WatchMetrics struct {
	SyncedWatchesTotal   prometheus.GaugeFunc
	UnsyncedWatchesTotal prometheus.GaugeFunc

	OnCommitDurationSec prometheus.Histogram
}

func NewWatchmetrics(reg prometheus.Registerer, wi watchinfo) *WatchMetrics {
	factory := promauto.With(reg)

	return &WatchMetrics{
		SyncedWatchesTotal: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "watch",
			Name:      "synced_watches_total",
			Help:      "Current amount of synced watches.",
		}, func() float64 {
			return float64(wi.SyncedWatchesLen())
		}),
		UnsyncedWatchesTotal: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "watch",
			Name:      "unsynced_watches_total",
			Help:      "Current amount of unsynced watches.",
		}, func() float64 {
			return float64(wi.UnsyncedWatchesLen())
		}),
		OnCommitDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "watch",
			Name:      "on_commit_duration_sec",
			Help:      "Latency of sending every change to every watcher after fsm.Apply.",
		}),
	}
}
