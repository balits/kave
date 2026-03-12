package metrics

import (
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RaftMetrics struct {
	ElectionsTotal     prometheus.Counter
	LeaderChangesTotal prometheus.Counter
	ApplyTotal         prometheus.Counter

	IsLeader    prometheus.Gauge
	ApplyIndex  prometheus.GaugeFunc // derived from fsm.lastAppliedIndex
	CommitIndex prometheus.GaugeFunc // derived from fsm.commitIndex
	Lag         prometheus.GaugeFunc // derived from commitIndex - lastAppliedIndex
	Ready       prometheus.GaugeFunc

	ElectionDurationSec prometheus.Histogram
	ApplyDurationSec    prometheus.Histogram
}

func NewRaftMetrics(reg prometheus.Registerer, r *raft.Raft, applyLagThresholt uint) *RaftMetrics {
	factory := promauto.With(reg)
	return &RaftMetrics{
		ElectionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "elections_total",
			Help:      "Total number of leader elections initiated.",
		}),
		LeaderChangesTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "leader_changes_toatl",
			Help:      "Total number of leader changes.",
		}),
		ApplyTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "apply_total",
			Help:      "Total number of Apply calls from Raft to the state machine.",
		}),

		IsLeader: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "is_leader",
			Help:      "Whether this node is currently the leader (1 for leader, 0 for follower).",
		}),
		ApplyIndex: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "apply_index",
			Help:      "Index of the last log entry applied to the state machine.",
		}, func() float64 {
			return float64(r.AppliedIndex())
		}),
		CommitIndex: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "commit_index",
			Help:      "Index of the last log entry commited to the cluster.",
		}, func() float64 {
			return float64(r.CommitIndex())
		}),
		Lag: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "lag",
			Help:      "Lag between commit index and applied index.",
		}, func() float64 {
			return float64(r.CommitIndex() - r.AppliedIndex())
		}),
		Ready: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "ready",
			Help:      "Whether the node is ready to serve requests (leader and not lagging behind). 1 is we are ready, 0 if not.",
		}, func() float64 {
			if r.State() == raft.Shutdown {
				return 0
			}
			if r.CommitIndex()-r.AppliedIndex() > uint64(applyLagThresholt) {
				return 0
			}
			return 1
		}),

		ElectionDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "election_duration_seconds",
			Help:      "Time taken to elect a leader.",
			// or custom: []float64{.005, .01, .025, .05, .1, .25, .5, 1}
			Buckets: prometheus.DefBuckets,
		}),
		ApplyDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "apply_duration_seconds",
			Help:      "Time taken to apply a command to the state machine.",
			// or custom: []float64{.001, .005, .01, .05, .1, .5, 1}
			Buckets: prometheus.DefBuckets,
		}),
	}
}
