package fsm

import (
	"context"
	"errors"
	"log/slog"

	"github.com/balits/kave/internal/metrics"
	"github.com/hashicorp/raft"
)

type RaftEventWatcher struct {
	c                   chan raft.Observation
	metrics             *metrics.RaftMetrics
	myID                raft.ServerID
	filterFn            raft.FilterFn
	leadershipObservers []LeadershipObserver
	logger              *slog.Logger
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
	w.leadershipObservers = append(w.leadershipObservers, obs...)
}

func (w *RaftEventWatcher) FilterFn() raft.FilterFn {
	return w.filterFn
}

func (w *RaftEventWatcher) Run(ctx context.Context) error {
	for {
		var (
			ev raft.Observation
			ok bool
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok = <-w.c:
			if !ok {
				return errors.New("observation channel closed")
			}
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

			for _, ob := range w.leadershipObservers {
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

var defaultFilterFn = func(ev *raft.Observation) bool {
	switch ev.Data.(type) {
	case raft.RaftState, raft.LeaderObservation:
		return true
	}
	return false
}
