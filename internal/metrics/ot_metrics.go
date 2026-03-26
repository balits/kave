package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type OTMetrics struct {
	WriteAllCount prometheus.Counter
	InitCount     prometheus.Counter
	TransferCount prometheus.Counter

	WriteAllErrorsTotal prometheus.Counter
	InitErrorsTotal     prometheus.Counter
	TransferErrorsTotal prometheus.Counter

	WriteAllDurationSec prometheus.Histogram
	InitDurationSec     prometheus.Histogram
	TransferDurationSec prometheus.Histogram
}

func NewOTMetrics(reg prometheus.Registerer) *OTMetrics {
	factory := promauto.With(reg)

	return &OTMetrics{
		WriteAllCount: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "write_all_count",
			Help:      "Total number of WriteAll calls.",
		}),
		InitCount: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "init_count",
			Help:      "Total number of Init calls.",
		}),
		TransferCount: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "transfer_count",
			Help:      "Total number of Transfer calls.",
		}),
		WriteAllErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "write_all_errors_total",
			Help:      "Total number of errors in WriteAll calls.",
		}),
		InitErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "init_errors_total",
			Help:      "Total number of errors in Init calls.",
		}),
		TransferErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "transfer_errors_total",
			Help:      "Total number of errors in Transfer calls.",
		}),
		WriteAllDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "write_all_duration_sec",
			Help:      "Duration of WriteAll calls in seconds.",
		}),
		InitDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "init_duration_sec",
			Help:      "Duration of Init calls in seconds.",
		}),
		TransferDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "ot",
			Name:      "transfer_duration_sec",
			Help:      "Duration of Transfer calls in seconds.",
		}),
	}
}
