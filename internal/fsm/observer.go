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
}

func NewRaftEventWatcher(c chan raft.Observation, m *metrics.RaftMetrics, id raft.ServerID) *RaftEventWatcher {
	return &RaftEventWatcher{
		c:        c,
		metrics:  m,
		myID:     id,
		filterFn: defaultFilterFn,
	}
}

func (ew *RaftEventWatcher) RegisterLeadershipObservers(obs ...LeadershipObserver) {
	ew.obs = append(ew.obs, obs...)
}

func (ew *RaftEventWatcher) FilterFn() raft.FilterFn {
	return ew.filterFn
}

func (ew *RaftEventWatcher) Run() {
	for {
		ev, ok := <-ew.c
		if !ok {
			return
		}

		switch eventType := ev.Data.(type) {
		case raft.RaftState:
			if eventType == raft.Candidate {
				ew.metrics.ElectionsTotal.Inc()
			}
		case raft.LeaderObservation:
			ew.metrics.LeaderChangesTotal.Inc()
			leadershipGranted := eventType.LeaderID == ew.myID

			if leadershipGranted {
				ew.metrics.IsLeader.Inc()
			} else {
				ew.metrics.IsLeader.Dec()
			}

			for _, ob := range ew.obs {
				if leadershipGranted {
					ob.OnLeadershipGranted()
				} else {
					ob.OnLeadershipLost()
				}
			}
		}
	}

}

func (ew *RaftEventWatcher) Stop() {
	close(ew.c)
}
