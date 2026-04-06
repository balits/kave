package metrics

import (
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RaftMetrics struct {
	raftLibDerivedMetrics

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
		raftLibDerivedMetrics: newRaftLibDerivedMetrics(reg, r),

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
			// or custom buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1}
		}),
		ApplyDurationSec: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "apply_duration_seconds",
			Help:      "Time taken to apply a command to the state machine.",
			// or custom buckets: []float64{.001, .005, .01, .05, .1, .5, 1}
		}),
	}
}

type raftLibDerivedMetrics struct {
	RaftState         prometheus.GaugeFunc // UnknownError = -1, follower = 0, candidate = 1, leader = 2, Shutdown 3
	RaftTerm          prometheus.GaugeFunc
	LastLogIndex      prometheus.GaugeFunc
	LastLogTerm       prometheus.GaugeFunc
	CommitIndex       prometheus.GaugeFunc
	AppliedIndex      prometheus.GaugeFunc
	FsmPending        prometheus.GaugeFunc
	LastSnapshotIndex prometheus.GaugeFunc
	LastSnapshotTerm  prometheus.GaugeFunc
}

func newRaftLibDerivedMetrics(reg prometheus.Registerer, r *raft.Raft) raftLibDerivedMetrics {
	var (
		factory = promauto.With(reg)
	)

	raftTerm := func() float64 {
		stats := r.Stats()
		return parseUint(stats["term"])
	}
	lastLogIndex := func() float64 {
		stats := r.Stats()
		return parseUint(stats["last_log_index"])
	}
	lastLogTerm := func() float64 {
		stats := r.Stats()
		return parseUint(stats["last_log_term"])
	}
	commitIndex := func() float64 {
		stats := r.Stats()
		return parseUint(stats["commit_index"])
	}
	appliedIndex := func() float64 {
		stats := r.Stats()
		return parseUint(stats["applied_index"])
	}
	fsmPending := func() float64 {
		stats := r.Stats()
		return parseUint(stats["fsm_pending"])
	}
	lastSnapshotIndex := func() float64 {
		stats := r.Stats()
		return parseUint(stats["last_snapshot_index"])
	}
	lastSnapshotTerm := func() float64 {
		stats := r.Stats()
		return parseUint(stats["last_snapshot_term"])
	}

	raftState := func() float64 {
		stats := r.Stats()
		switch stats["state"] {
		case "Follower":
			return float64(0)
		case "Candidate":
			return float64(1)
		case "Leader":
			return float64(2)
		case "Shutdown":
			return float64(3)
		default:
			// UnknownError
			return float64(-1)
		}
	}

	return raftLibDerivedMetrics{
		RaftState: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "state",
			Help:      "Current state of the Raft node (0=follower, 1=candidate, 2=leader, 3=shutdown, -1=unknown/error).",
		}, raftState),
		RaftTerm: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "term",
			Help:      "Current term of the Raft node.",
		}, raftTerm),
		LastLogIndex: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "last_log_index",
			Help:      "Index of the last log entry.",
		}, lastLogIndex),
		LastLogTerm: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "last_log_term",
			Help:      "Term of the last log entry.",
		}, lastLogTerm),
		CommitIndex: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "cobmmit_index_lib",
			Help:      "Highest commited log entry.",
		}, commitIndex),
		AppliedIndex: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "applied_index_lib",
			Help:      "Index of the last log entry applied to the FSM.",
		}, appliedIndex),
		FsmPending: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "fsm_pending",
			Help:      "Number of FSM commands pending.",
		}, fsmPending),
		LastSnapshotIndex: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "last_snapshot_index_lib",
			Help:      "Index of the last snapshot.",
		}, lastSnapshotIndex),
		LastSnapshotTerm: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "last_snapshot_term_lib",
			Help:      "Term of the last snapshot.",
		}, lastSnapshotTerm),
	}
}

func parseUint(s string) float64 {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return -1
	}
	return float64(u)
}
