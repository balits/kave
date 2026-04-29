package metrics

import (
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RaftMetrics struct {
	ElectionsTotal     prometheus.Counter
	LeaderChangesTotal prometheus.Counter
	ApplyTotal         prometheus.Counter

	IsLeader    prometheus.GaugeFunc
	ApplyIndex  prometheus.GaugeFunc // derived from fsm.lastAppliedIndex
	CommitIndex prometheus.GaugeFunc // derived from fsm.commitIndex
	Lag         prometheus.GaugeFunc // derived from commitIndex - lastAppliedIndex

	ElectionDurationSec prometheus.Histogram
	ApplyDurationSec    prometheus.Histogram
}

func NewRaftMetrics(reg prometheus.Registerer, r *raft.Raft, applyLagThresholt uint) *RaftMetrics {
	factory := promauto.With(reg)
	reg.MustRegister(newRaftStatsCollector(r))

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
			Name:      "leader_changes_total",
			Help:      "Total number of leader changes.",
		}),
		ApplyTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "apply_total",
			Help:      "Total number of Apply calls from Raft to the state machine.",
		}),

		IsLeader: factory.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "kave",
			Subsystem: "raft",
			Name:      "is_leader",
			Help:      "Whether this node is currently the leader (1 for leader, 0 for follower).",
		}, func() float64 {
			if r.State() == raft.Leader {
				return 1
			}
			return 0
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

	return raftLibDerivedMetrics{
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
			Name:      "commmit_index_lib",
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

// raftStatsCollector implements prometheus.Collector
// to fetch all Raft stats with a single r.Stats() call.
// r.Stats() stalls the fsm, and previously we called it
// on every gaugeFunc
type raftStatsCollector struct {
	r *raft.Raft

	stateDesc             *prometheus.Desc
	termDesc              *prometheus.Desc
	lastLogIndexDesc      *prometheus.Desc
	lastLogTermDesc       *prometheus.Desc
	commitIndexDesc       *prometheus.Desc
	appliedIndexDesc      *prometheus.Desc
	fsmPendingDesc        *prometheus.Desc
	lastSnapshotIndexDesc *prometheus.Desc
	lastSnapshotTermDesc  *prometheus.Desc
}

func newRaftStatsCollector(r *raft.Raft) *raftStatsCollector {
	return &raftStatsCollector{
		r: r,
		stateDesc: prometheus.NewDesc(
			"kave_raft_state",
			"Current state of the Raft node (0=follower, 1=candidate, 2=leader, 3=shutdown, -1=unknown/error).",
			nil, nil,
		),
		termDesc: prometheus.NewDesc(
			"kave_raft_term",
			"Current term of the Raft node.",
			nil, nil,
		),
		lastLogIndexDesc: prometheus.NewDesc(
			"kave_raft_last_log_index",
			"Index of the last log entry.",
			nil, nil,
		),
		lastLogTermDesc: prometheus.NewDesc(
			"kave_raft_last_log_term",
			"Term of the last log entry.",
			nil, nil,
		),
		commitIndexDesc: prometheus.NewDesc(
			"kave_raft_commit_index_lib",
			"Highest commited log entry.",
			nil, nil,
		),
		appliedIndexDesc: prometheus.NewDesc(
			"kave_raft_applied_index_lib",
			"Index of the last log entry applied to the FSM.",
			nil, nil,
		),
		fsmPendingDesc: prometheus.NewDesc(
			"kave_raft_fsm_pending",
			"Number of FSM commands pending.",
			nil, nil,
		),
		lastSnapshotIndexDesc: prometheus.NewDesc(
			"kave_raft_last_snapshot_index_lib",
			"Index of the last snapshot.",
			nil, nil,
		),
		lastSnapshotTermDesc: prometheus.NewDesc(
			"kave_raft_last_snapshot_term_lib",
			"Term of the last snapshot.",
			nil, nil,
		),
	}
}

func (c *raftStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.stateDesc
	ch <- c.termDesc
	ch <- c.lastLogIndexDesc
	ch <- c.lastLogTermDesc
	ch <- c.commitIndexDesc
	ch <- c.appliedIndexDesc
	ch <- c.fsmPendingDesc
	ch <- c.lastSnapshotIndexDesc
	ch <- c.lastSnapshotTermDesc
}

// Collect calls r.Stats() once, instead of calling it in every gaugeFunc
func (c *raftStatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.r.Stats()

	var stateVal float64
	switch stats["state"] {
	case "Follower":
		stateVal = 0
	case "Candidate":
		stateVal = 1
	case "Leader":
		stateVal = 2
	case "Shutdown":
		stateVal = 3
	default:
		stateVal = -1
	}

	ch <- prometheus.MustNewConstMetric(c.stateDesc, prometheus.GaugeValue, stateVal)
	ch <- prometheus.MustNewConstMetric(c.termDesc, prometheus.GaugeValue, parseUint(stats["term"]))
	ch <- prometheus.MustNewConstMetric(c.lastLogIndexDesc, prometheus.GaugeValue, parseUint(stats["last_log_index"]))
	ch <- prometheus.MustNewConstMetric(c.lastLogTermDesc, prometheus.GaugeValue, parseUint(stats["last_log_term"]))
	ch <- prometheus.MustNewConstMetric(c.commitIndexDesc, prometheus.GaugeValue, parseUint(stats["commit_index"]))
	ch <- prometheus.MustNewConstMetric(c.appliedIndexDesc, prometheus.GaugeValue, parseUint(stats["applied_index"]))
	ch <- prometheus.MustNewConstMetric(c.fsmPendingDesc, prometheus.GaugeValue, parseUint(stats["fsm_pending"]))
	ch <- prometheus.MustNewConstMetric(c.lastSnapshotIndexDesc, prometheus.GaugeValue, parseUint(stats["last_snapshot_index"]))
	ch <- prometheus.MustNewConstMetric(c.lastSnapshotTermDesc, prometheus.GaugeValue, parseUint(stats["last_snapshot_term"]))
}
