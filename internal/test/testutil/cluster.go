package testutil

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/hashicorp/raft"
)

func NewMockDurableCluster(tempdir string, baseCfg *config.Config, logger *slog.Logger) ([]*raftnode.Node, error) {
	var (
		wg        sync.WaitGroup
		nAttempts = 6
		nodes     = make([]*raftnode.Node, len(baseCfg.ClusterInfo))
	)

	startNode := func(config *config.Config, nodeInfo config.ServiceInfo, nodeIndex int, tempdir string) {
		defer wg.Done()

		fsm := NewMockInmemFSM()
		stores, err := raftnode.NewRaftStores(tempdir, nodeInfo.RaftID, false)
		check(err)
		node, err := raftnode.NewNode(config, fsm, stores, logger.With("component", nodeInfo.RaftID))
		check(err)
		nodes[nodeIndex] = node

		server := api.NewServer(nodeInfo.GetInternalHttpAddress(), node, logger.With("component", "httpserver"))
		server.RegisterRoutes()
		go server.Run()

		check(node.BootstrapOrJoinCluster(nAttempts))

	}
	leaderIdx := -1
	for i, info := range baseCfg.ClusterInfo {
		if info.NeedBootstrap {
			leaderIdx = i
			go startNode(NewConfigForNode(baseCfg, info), info, i, filepath.Join(tempdir, info.RaftID))
		}
	}
	if leaderIdx == -1 {
		return nil, fmt.Errorf("no bootstrapping node belongs to the cluster")
	}

	check(WaitUntilLeaderReady(nodes[time.Duration(leaderIdx)], 10*time.Second))

	for i, info := range baseCfg.ClusterInfo {
		if !info.NeedBootstrap {
			go startNode(NewConfigForNode(baseCfg, info), info, i, filepath.Join(tempdir, info.RaftID))
		}
	}

	// // leader -> started bootstrapping, but it takes time to elect itself as leader
	// // others -> they only return after successfully joined, or joing timed out (with exponential backoff up to nAttempts times)
	// //        -> we can assume that timing out with a sufficienlty large nAttempts is enough for the leader to elect itself
	// wg.Wait()

	return nodes, nil
}

func DiscoverCondition(t *testing.T, services []*raftnode.Node) bool {
	for _, svc := range services {
		addr, ID := svc.Raft.LeaderWithID()
		// t.Logf("node: %s checking for current leader: %s, %s", svc.Config.ThisService.RaftID, addr, ID)
		if string(addr) == "" || string(ID) == "" {
			// t.Errorf("Service %s did not join cluster: leader not found", svc.Config.ThisService.RaftID)
			return false
		}
	}
	return true
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func WaitUntilLeaderReady(n *raftnode.Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.Raft.State() == raft.Leader {
			f := n.Raft.GetConfiguration()
			if err := f.Error(); err == nil {
				cfg := f.Configuration()
				hasSelf := false
				for _, s := range cfg.Servers {
					if s.ID == raft.ServerID(n.Config.ThisService.RaftID) &&
						s.Address == raft.ServerAddress(n.Config.ThisService.GetRaftAddress()) &&
						s.Suffrage == raft.Voter {
						hasSelf = true
						break
					}
				}
				if hasSelf {
					// optional: ensure leadership ops are processed
					if err := n.Raft.Barrier(0).Error(); err == nil {
						return nil
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("leader not ready before timeout")
}
