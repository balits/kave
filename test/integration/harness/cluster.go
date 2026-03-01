// Core raft properties to test:
//
// - Leader election works
//
// - Writes go through leader
//
// - Replication reaches quorum
//
// - Followers reject direct writes
//
// - Leader failover works
//
// - Old leader steps down
//
// - Snapshot creation works
//
// - Snapshot install works
//
// - Restart recovery works
//
// - No split brain
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/balits/kave/internal/common"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/node"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/util"
	"github.com/stretchr/testify/require"
)

type Cluster struct {
	t        testing.TB
	Ctx      context.Context
	Cancel   context.CancelFunc
	Nodes    []*node.Node
	dataDirs []string
}

func NewTestCluster(t testing.TB, numNodes int) *Cluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	cluster := &Cluster{t: t, Ctx: ctx, Cancel: cancel}

	peers := make([]common.Peer, 0, numNodes)
	for i := range numNodes {
		peers = append(peers, common.Peer{
			NodeID:   fmt.Sprintf("node%d", i),
			Hostname: "127.0.0.1",
			RaftPort: randomPort(t),
			HttpPort: randomPort(t),
		})
	}

	for i := range numNodes {
		dir := t.TempDir()
		cfg := &config.Config{
			Bootstrap:   i == 0,
			Dir:         dir,
			Me:          peers[i],
			StorageKind: storage.StorageKindInMemory,
			Peers:       peers,
			LogLevel:    slog.LevelDebug,
		}
		logger := util.NewLoggerWithKind(cfg.LogLevel, os.Stdout, util.TextLoggerKind)
		n, err := node.New(cfg, logger)
		require.NoErrorf(t, err, "Failed to create node %d: %v", i, err)
		cluster.Nodes = append(cluster.Nodes, n)
		cluster.dataDirs = append(cluster.dataDirs, dir)
	}

	for i, n := range cluster.Nodes {
		go func(ctx context.Context, node *node.Node, idx int) {
			err := node.Run(ctx)
			if err != nil {
				t.Logf("Node %d exited with error: %v", idx, err)
			} else {
				t.Logf("Node %d exited successfully", idx)
			}
		}(cluster.Ctx, n, i)
	}

	return cluster
}

func (c *Cluster) WaitForLeader() (*node.Node, error) {
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n.IsLeader() {
				return n, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil, fmt.Errorf("no leader elected within timeout")
}

func (c *Cluster) Shutdown() {
	time.AfterFunc(time.Minute, c.Cancel)
}

func randomPort(tb testing.TB) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("failed to create server: %v", err)
	}
	addr := ln.Addr()
	if err = ln.Close(); err != nil {
		tb.Fatalf("failed to close listener: %v", err)
	}
	tb.Log("new random addr:", addr)
	return strings.Split(addr.String(), ":")[1]
}
