package testx

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func WaitForState(node *TestNode, s raft.RaftState) error {
	limit := time.Now().Add(WaitStateTimeout)
	for time.Now().Before(limit) {
		if node.Raft.State() == s {
			node.Logger.Debug("WaitForState OK", "state", s, "nodeID", node.Config.NodeID)
			return nil
		}
		time.Sleep(WaitStateTimeout / 10)
	}
	return NewErrTimeout(WaitStateTimeout, fmt.Sprintf("WaitForState(wanted %s, got %s)", s, node.Raft.State()))
}

func WaitForFuture(t testing.TB, f raft.Future) error {
	timer := time.AfterFunc(WaitFutureTimeout, func() {
		err := NewErrTimeout(WaitFutureTimeout, "WaitForFuture")
		t.Fatal(err)
	})
	defer timer.Stop()
	return f.Error()
}

func WaitForNode(s raft.RaftState, nodes []*TestNode) (*TestNode, error) {
	timeout := WaitStateTimeout
	limit := time.Now().Add(timeout)
	sleep := timeout / 10
	for time.Now().Before(limit) {
		for _, n := range nodes {
			if n.Raft.State() == s {
				n.Logger.Debug("WaitForNode OK", "state", s, "nodeID", n.Config.NodeID)
				return n, nil
			}
		}
		time.Sleep(sleep)
	}

	return nil, NewErrTimeout(timeout, "WaitForNode")
}
