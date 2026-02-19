package integration

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/testx"
	"github.com/hashicorp/raft"
)

func TestClusterAction_3NodeCluster_ApplyChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cluster := makeCluster(t, ctx, 3)
	defer cluster.Shutdown()
	nodes := cluster.Slice()

	setCommandsLen := 10
	setCommands := make([]command.Command, 0, setCommandsLen)
	for i := range setCommandsLen {
		setCommands = append(setCommands, command.Command{
			Type:  command.CommandTypeSet,
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		})
	}

	leader, err := testx.WaitForNode(raft.Leader, nodes)
	t.Logf("leader: %s", leader.ToString())
	testx.NoErr(t, err)

	totalApplied := applyCmdsAndWait(t, leader, setCommands)
	t.Logf("total applied: %d", totalApplied)

	testx.NoErr(t, testx.WaitForFuture(t, leader.Raft.Snapshot()))

	time.Sleep(2 * time.Second)
	debugFsms(nodes)

	testx.CheckConsistent(t, nodes)
}

func makeCluster(tb testing.TB, ctx context.Context, n int) *testx.Cluster {
	cluster := testx.NewCluster(tb, ctx)

	node := testx.NewTestNode(tb, ctx, "node1")
	testx.NoErr(tb, cluster.Bootstrap(node))
	testx.NoErr(tb, testx.WaitForState(node, raft.Leader))

	for i := 2; i <= n; i++ {
		id := fmt.Sprintf("node%d", i)
		node2 := testx.NewTestNode(tb, ctx, id)
		testx.NoErr(tb, cluster.Join(node2))
		testx.NoErr(tb, testx.WaitForState(node2, raft.Follower))
	}

	leader, err := testx.WaitForNode(raft.Leader, cluster.Slice())
	testx.NoErr(tb, err)

	for _, n := range cluster.Slice() {
		if leader.Config.NodeID == n.Config.NodeID {
			continue
		}
		testx.NoErr(tb, testx.WaitForState(n, raft.Follower))
	}

	return cluster
}

func applyCmdsAndWait(tb testing.TB, node *testx.TestNode, commands []command.Command) int {
	var (
		futures []raft.ApplyFuture
	)

	for _, cmd := range commands {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
			tb.Fatal(err)
		}
		futures = append(futures, node.Raft.Apply(buf.Bytes(), 0))
	}

	for i, f := range futures {
		testx.NoErr(tb, testx.WaitForFuture(tb, f))
		node.Logger.Debug("applied", "index", f.Index(), "size", i)
	}

	return len(futures)
}
