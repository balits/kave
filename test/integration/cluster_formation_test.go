package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/balits/thesis/internal/testx"
	"github.com/hashicorp/raft"
)

func TestClusterFormation_BootstrapSingleNode(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	node := testx.NewTestNode(t, ctx, "node1")
	testx.NoErr(t, node.Bootstrap())
	defer node.Release()

	testx.NoErr(t, testx.WaitForState(node, raft.Leader))
	leader, err := testx.WaitForNode(raft.Leader, []*testx.TestNode{node})
	testx.NoErr(t, err)

	leader.Release()
	leader.Restart(t)

	testx.NoErr(t, testx.WaitForState(node, raft.Leader))
	_, err = testx.WaitForNode(raft.Leader, []*testx.TestNode{node})
	testx.NoErr(t, err)
}

func TestClusterFormation_ClusterBootstrap(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cluster := testx.NewCluster(t, ctx)
	defer cluster.Shutdown()

	node := testx.NewTestNode(t, ctx, "node1")
	testx.NoErr(t, cluster.Bootstrap(node))

	testx.NoErr(t, testx.WaitForState(node, raft.Leader))
	_, err := testx.WaitForNode(raft.Leader, []*testx.TestNode{node})
	testx.NoErr(t, err)
}

func TestClusterFormation_JoinOneNode(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cluster := testx.NewCluster(t, ctx)
	defer cluster.Shutdown()

	node := testx.NewTestNode(t, ctx, "node1")
	testx.NoErr(t, cluster.Bootstrap(node))
	testx.NoErr(t, testx.WaitForState(node, raft.Leader))

	id2 := fmt.Sprintf("node%d", 2)
	node2 := testx.NewTestNode(t, ctx, id2)
	testx.NoErr(t, cluster.Join(node2))
	testx.NoErr(t, testx.WaitForState(node2, raft.Follower))

	flw, err := testx.WaitForNode(raft.Follower, cluster.Slice())
	testx.NoErr(t, err)
	if flw.Config.NodeID != id2 {
		t.Fatalf("expected follower to be %s, got %s", id2, flw.Config.NodeID)
	}
}

func TestClusterFormation_JoinMultipleNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cluster := testx.NewCluster(t, ctx)
	defer cluster.Shutdown()

	node := testx.NewTestNode(t, ctx, "node1")
	testx.NoErr(t, cluster.Bootstrap(node))
	testx.NoErr(t, testx.WaitForState(node, raft.Leader))

	for i := 2; i <= 5; i++ {
		id := fmt.Sprintf("node%d", i)
		node2 := testx.NewTestNode(t, ctx, id)
		testx.NoErr(t, cluster.Join(node2))
		testx.NoErr(t, testx.WaitForState(node2, raft.Follower))
	}

	leader, err := testx.WaitForNode(raft.Leader, cluster.Slice())
	testx.NoErr(t, err)

	for _, n := range cluster.Slice() {
		if leader.Config.NodeID == n.Config.NodeID {
			continue
		}
		testx.NoErr(t, testx.WaitForState(n, raft.Follower))
	}
}
