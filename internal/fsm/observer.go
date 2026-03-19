package fsm

import (
	"log/slog"

	"github.com/balits/kave/internal/metrics"
	"github.com/hashicorp/raft"
)

var defaultFilterFn = func(ev *raft.Observation) bool {
	switch ev.Data.(type) {
	case raft.RaftState, raft.LeaderObservation:
		return true
	}
	return false
}

type LeadershipObserver interface {
	OnLeadershipGranted()
	OnLeadershipLost()
}

type RaftEventWatcher struct {
	c        chan raft.Observation
	metrics  *metrics.RaftMetrics
	myID     raft.ServerID
	filterFn raft.FilterFn
	obs      []LeadershipObserver
	logger   *slog.Logger
}

func NewRaftEventWatcher(logger *slog.Logger, c chan raft.Observation, m *metrics.RaftMetrics, id raft.ServerID) *RaftEventWatcher {
	return &RaftEventWatcher{
		c:        c,
		metrics:  m,
		myID:     id,
		filterFn: defaultFilterFn,
		logger:   logger.With("component", "raft_event_watcher"),
	}
}

func (w *RaftEventWatcher) RegisterLeadershipObservers(obs ...LeadershipObserver) {
	w.obs = append(w.obs, obs...)
}

func (w *RaftEventWatcher) FilterFn() raft.FilterFn {
	return w.filterFn
}

func (w *RaftEventWatcher) Run() {
	for {
		ev, ok := <-w.c
		if !ok {
			return
		}

		switch eventType := ev.Data.(type) {
		case raft.RaftState:
			if eventType == raft.Candidate {
				w.metrics.ElectionsTotal.Inc()
				w.logger.Debug("election")
			}
		case raft.LeaderObservation:
			w.metrics.LeaderChangesTotal.Inc()
			leadershipGranted := eventType.LeaderID == w.myID
			w.logger.Debug("leadership", "granted", leadershipGranted)

			if leadershipGranted {
				w.metrics.IsLeader.Inc()
			} else {
				w.metrics.IsLeader.Dec()
			}

			for _, ob := range w.obs {
				if leadershipGranted {
					ob.OnLeadershipGranted()
				} else {
					ob.OnLeadershipLost()
				}
			}
		}
	}

}

func (w *RaftEventWatcher) Stop() {
	close(w.c)
}
