package metrics

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type gaugeFn func() float64

func InitPrometheus() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

func InitTestPrometheus() *prometheus.Registry {
	if !testing.Testing() {
		panic("InitTestPrometheus should only be used in tests")
	}
	return prometheus.NewRegistry()
}

type RaftMetrics struct {
	ElectionsTotal     prometheus.Counter
	LeaderChangesTotal prometheus.Counter
	ApplyTotal         prometheus.Counter

	IsLeader    prometheus.Gauge
	ApplyIndex  prometheus.GaugeFunc // derived from fsm.lastAppliedIndex
	CommitIndex prometheus.GaugeFunc // derived from fsm.commitIndex
	Lag         prometheus.GaugeFunc // derived from commitIndex - lastAppliedIndex
	Ready       prometheus.GaugeFunc

	LeaderElectionDuration prometheus.Histogram
	ApplyLatency           prometheus.Histogram
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
			Namespace: "kvstore",
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

		LeaderElectionDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kvstore",
			Subsystem: "raft",
			Name:      "election_duration_seconds",
			Help:      "Time taken to elect a leader.",
			// or custom: []float64{.005, .01, .025, .05, .1, .25, .5, 1}
			Buckets: prometheus.DefBuckets,
		}),
		ApplyLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kvstore",
			Subsystem: "raft",
			Name:      "apply_latency_seconds",
			Help:      "Time taken to apply a command to the state machine.",
			// or custom: []float64{.001, .005, .01, .05, .1, .5, 1}
			Buckets: prometheus.DefBuckets,
		}),
	}
}

type KVMetrics struct {
	// TODO: somehow we need keep track of keys, but we cannot do it easily
	// cuz we batch writes deep inside the storage layer, maybe use observers ???
	KeyCount      prometheus.Gauge
	LeaseCount    prometheus.Gauge
	WatchersCount prometheus.Gauge

	// derived from mvcc.KVStore
	CurrentRev   prometheus.GaugeFunc
	CompactedRev prometheus.GaugeFunc

	// errors per cmd
	ReadErrorsTotal   prometheus.Counter
	PutErrorsTotal    prometheus.Counter
	DeleteErrorsTotal prometheus.Counter
	CompactionErrors  prometheus.Counter

	// cmd counts
	ReadsTotal prometheus.Counter
	// PutsTotal    prometheus.Counter
	// DeletesTotal prometheus.Counter
	TxnsTotal        prometheus.Counter // count of committed transactions
	CompactionsTotal prometheus.Counter // count of compactions

	ReadLatency prometheus.Histogram
	// PutLatency    prometheus.Histogram
	// DeleteLatency prometheus.Histogram
	TxnLatency        prometheus.Histogram // lifetime of a mvcc.writer
	CompactionLatency prometheus.Histogram
}

func NewKVMetrics(
	reg prometheus.Registerer,
	currentRevFn gaugeFn,
	compactedRevFn gaugeFn,
) *KVMetrics {
	factory := promauto.With(reg)

	return &KVMetrics{
		KeyCount: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "key_count",
			Help:      "Current number of keys in the store.",
		}),
		LeaseCount: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "lease_count",
			Help:      "Current number of leases in the store.",
		}),
		WatchersCount: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "watcher_count",
			Help:      "Current number of watchers in the store.",
		}),
		CurrentRev: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "current_revision",
			Help:      "Current main revision of the store.",
		}, currentRevFn),
		CompactedRev: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compacted_revision",
			Help:      "Revision up to which the store has been compacted.",
		}, compactedRevFn),

		ReadErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "read_errors_total",
			Help:      "Total errors during Read operations.",
		}),
		PutErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "put_errors_total",
			Help:      "Total errors during PUT operations.",
		}),
		DeleteErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "delete_errors_total",
			Help:      "Total errors during DEL operations.",
		}),
		CompactionErrors: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compaction_errors_total",
			Help:      "Total errors during compaction.",
		}),

		ReadsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "reads_total",
			Help:      "Total Read operations.",
		}),
		TxnsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "txns_total",
			Help:      "Total TXN operations.",
		}),
		CompactionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compactions_total",
			Help:      "Total compaction operations.",
		}),

		ReadLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "read_duration_seconds",
			Help:      "Latency of Read operations.",
			Buckets:   []float64{.0001, .0005, .001, .005, .01, .05, .1},
		}),
		TxnLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "txn_duration_seconds",
			Help:      "Lifetime of transactions from start to commit.",
			Buckets:   []float64{.0001, .0005, .001, .005, .01, .05, .1},
		}),
		CompactionLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "kv",
			Name:      "compaction_duration_seconds",
			Help:      "Latency of compaction operations.",
			Buckets:   []float64{.001, .005, .01, .05, .1, .5, 1},
		}),
	}
}

type BackendMetrics struct {
	SizeBytes prometheus.GaugeFunc // derived from

	CommitLatency prometheus.Histogram
	BatchSize     prometheus.Histogram // number of operations in a batch -> is batching even efficient?
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
		CommitLatency: factory.NewHistogram(prometheus.HistogramOpts{
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
