package fsm

import (
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

type RaftEventWatcher struct {
	c        chan raft.Observation
	metrics  *metrics.RaftMetrics
	myID     raft.ServerID
	filterFn raft.FilterFn
}

func NewRaftEventWatcher(c chan raft.Observation, m *metrics.RaftMetrics, id raft.ServerID) *RaftEventWatcher {
	return &RaftEventWatcher{
		c:        c,
		metrics:  m,
		myID:     id,
		filterFn: defaultFilterFn,
	}
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
			}
		case raft.LeaderObservation:
			w.metrics.LeaderChangesTotal.Inc()
			if eventType.LeaderID == w.myID {
				w.metrics.IsLeader.Inc()
			} else {
				w.metrics.IsLeader.Dec()
			}
		}
	}

}

func (w *RaftEventWatcher) Stop() {
	close(w.c)
}
