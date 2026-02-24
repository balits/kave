package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/balits/kave/internal/node"
	"github.com/balits/kave/test/integration/harness"
	"github.com/stretchr/testify/require"
)

func Test_LeaderElection(t *testing.T) {
	cluster := harness.NewTestCluster(t, 3)
	defer cluster.Shutdown()

	leader, err := cluster.WaitForLeader()
	require.NoError(t, err, "Failed to wait for leader")
	t.Logf("Leader elected: %s", leader.Cfg().Me.NodeID)
}

func Test_Replication(t *testing.T) {
	cluster := harness.NewTestCluster(t, 3)
	defer cluster.Shutdown()

	leader, err := cluster.WaitForLeader()
	require.NoError(t, err, "Failed to wait for leader")
	t.Logf("Leader elected: %s", leader.Cfg().Me.NodeID)

	kvs := make([]struct{ key, value string }, 10)
	for i := range cap(kvs) {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := leader.Set(context.Background(), key, value)
		require.NoErrorf(t, err, "Failed to put value")
		kvs[i] = struct{ key, value string }{key: key, value: value}
	}

	require.Eventually(t, func() bool {
		for _, n := range cluster.Nodes {
			for _, kv := range kvs {
				val, err := n.Get(context.Background(), kv.key)
				if err != nil || val != kv.value {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond, "Failed to replicate data to all nodes")
}

func Test_FollowerRejectWrite(t *testing.T) {
	cluster := harness.NewTestCluster(t, 3)
	defer cluster.Shutdown()

	leader, err := cluster.WaitForLeader()
	require.NoError(t, err, "Failed to wait for leader")
	t.Logf("Leader elected: %s", leader.Cfg().Me.NodeID)

	for _, n := range cluster.Nodes {
		if n.Cfg().Me.NodeID != leader.Cfg().Me.NodeID {
			err := n.Set(context.Background(), "key", "value")
			require.Error(t, err, "Expected error when writing to follower")
		}
	}
}

func Test_LeaderFailover(t *testing.T) {
	cluster := harness.NewTestCluster(t, 3)
	defer cluster.Shutdown()

	leader, err := cluster.WaitForLeader()
	require.NoError(t, err, "Failed to wait for leader")
	t.Logf("Leader elected: %s", leader.Cfg().Me.NodeID)

	require.NoError(t, leader.Shutdown(context.Background()), "Failed to shutdown leader")

	newLeader, err := cluster.WaitForLeader()
	require.NoError(t, err, "Failed to wait for new leader")
	t.Logf("New leader elected: %s", newLeader.Cfg().Me.NodeID)
}

func Test_NodeRestart(t *testing.T) {
	cluster := harness.NewTestCluster(t, 3)
	defer cluster.Shutdown()

	leader, err := cluster.WaitForLeader()
	require.NoError(t, err, "Failed to wait for leader")
	t.Logf("Leader elected: %s", leader.Cfg().Me.NodeID)
	require.NoError(t, leader.Set(context.Background(), "key", "value"), "Failed to put value")

	var follower *node.Node
	for _, n := range cluster.Nodes {
		if n.Cfg().Me.NodeID != leader.Cfg().Me.NodeID {
			follower = n
			break
		}
	}

	require.NoError(t, follower.Shutdown(context.Background()), "Failed to shutdown follower")
	require.NoError(t, follower.Run(cluster.Ctx), "Failed to restart follower")

	require.Eventually(t, func() bool {
		val, err := follower.Get(context.Background(), "key")
		return err == nil && val == "value"
	}, 5*time.Second, 100*time.Millisecond, "Failed to recover state after restart")
}

// func Test_SnapshotCreation(t *testing.T) {
// 	cluster := harness.NewTestCluster(t, 3)
// 	defer cluster.Shutdown()

// 	leader, err := cluster.WaitForLeader()
// 	require.NoError(t, err, "Failed to find leader")

// 	// 1️⃣ Write enough entries to exceed threshold
// 	for i := 0; i < 200; i++ {
// 		key := fmt.Sprintf("k%d", i)
// 		val := fmt.Sprintf("v%d", i)
// 		err := leader.Set(context.Background(), key, val)
// 		require.NoError(t, err)
// 	}

// 	// 2️⃣ Wait for snapshot creation
// 	require.Eventually(t, func() bool {
// 		return leader.HasSnapshot()
// 	}, 10*time.Second, 200*time.Millisecond)

// 	// 3️⃣ Ensure logs were compacted
// 	stats := leader.RaftStats()
// 	require.Less(t, stats.LogEntries, 200)

// 	// 4️⃣ Restart leader
// 	ctx := context.Background()
// 	require.NoError(t, leader.Shutdown(ctx))
// 	require.NoError(t, leader.Start(ctx))

// 	// 5️⃣ Verify data still exists
// 	require.Eventually(t, func() bool {
// 		for i := 0; i < 200; i++ {
// 			key := fmt.Sprintf("k%d", i)
// 			val, ok := leader.Get([]byte(key))
// 			if !ok || string(val) != fmt.Sprintf("v%d", i) {
// 				return false
// 			}
// 		}
// 		return true
// 	}, 5*time.Second, 100*time.Millisecond)
// }
