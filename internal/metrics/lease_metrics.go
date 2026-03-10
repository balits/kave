package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type LeaseMetrics struct {
	LeasesGranted    prometheus.Counter
	LeasesRevoked    prometheus.Counter
	LeasesExpired    *prometheus.CounterVec // expired as a result of the expiry loop <-> expired since it was revoked
	KeepAliveTotal   prometheus.Counter
	KeepAliveErrors  prometheus.Counter // spike would mean users are trying to keep alive expired leases, maybe ttl is too short
	CheckpointsTotal prometheus.Counter

	ActiveLeases prometheus.GaugeFunc
	LeasedKeys   prometheus.Gauge

	GrantDurationSec     prometheus.Histogram
	KeepAliveDurationSec prometheus.Histogram
	LeaseTTLAtExpirySec  prometheus.Histogram
	KeysPerRevoke        prometheus.Histogram
}

func NewLeaseMetrics(reg prometheus.Registerer, activeLeasesFunc func() int) *LeaseMetrics {
	factory := promauto.With(reg)
	return &LeaseMetrics{
		LeasesGranted: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "leases_granted_total",
			Help:      "Total number of leases granted.",
		}),
		LeasesRevoked: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "leases_revoked_total",
			Help:      "Total number of leases revoked.",
		}),
		LeasesExpired: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "leases_expired_total",
			Help:      "Total number of leases expired, labeled by reason.",
		}, []string{"reason"}),
		KeepAliveTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "keep_alive_total",
			Help:      "Total number of keep-alive requests.",
		}),
		KeepAliveErrors: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "keep_alive_errors_total",
			Help:      "Total number of keep-alive errors.",
		}),
		CheckpointsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "checkpoints_total",
			Help:      "Total number of checkpoints created.",
		}),

		ActiveLeases: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "active_leases",
			Help:      "Number of leases currently active.",
		}, func() float64 {
			return float64(activeLeasesFunc())
		}),
		LeasedKeys: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "leased_keys",
			Help:      "Number of keys currently leased.",
		}),

		GrantDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "grant_latency_seconds",
			Help:      "Latency of granting leases.",
			Buckets:   []float64{0.0005, 0.001, 0.002, 0.005, .010, .025, .05, .1, .25, .5, 1, 2, 5},
		}),
		KeepAliveDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "keep_alive_latency_seconds",
			Help:      "Latency of keep-alive operations.",
			Buckets:   []float64{0.0005, 0.001, 0.002, 0.005, .010, .025, .05, .1, .25, .5, 1, 2, 5},
		}),
		LeaseTTLAtExpirySec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "lease_ttl_at_expiry_seconds",
			Help:      "TTL of leases at the moment they expired.",
			Buckets:   []float64{0, 0.5, 1, 5, 10, 30, 60},
		}),
		KeysPerRevoke: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "lease",
			Name:      "keys_per_revoke",
			Help:      "Number of keys removed by each revoke operation.",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
	}
}
