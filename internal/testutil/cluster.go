package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/util"
	"github.com/hashicorp/raft"
)

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

type Cluster struct {
	nodes   []*raftnode.Node
	servers []*api.Server
	config  *config.Config
}

func NewTestCluster(tempdir string, config *config.Config) (*Cluster, error) {
	var (
		nodes   = make([]*raftnode.Node, len(config.ClusterInfo))
		servers = make([]*api.Server, len(config.ClusterInfo))
		logger  = util.NewJSONLogger("DEBUG", os.Stdout)
	)

	for i, info := range config.ClusterInfo {
		nodeConfig := NewConfigForNode(config, info)
		stores, err := raftnode.NewRaftStores(tempdir, info.RaftID, true)
		if err != nil {
			return nil, err
		}
		node, err := raftnode.NewNode(nodeConfig, NewMockFSM(), stores, logger.With("component", "raft_"+info.RaftID))
		if err != nil {
			return nil, err
		}

		server := api.NewServer(info.GetInternalHttpAddress(), node, logger.With("component", "http_"+info.RaftID))
		server.RegisterRoutes()
		go server.Run()

		if err := node.BootstrapOrJoinCluster(5); err != nil {
			return nil, err
		}

		nodes[i] = node
		servers[i] = server
	}

	cluster := &Cluster{
		nodes:   nodes,
		servers: servers,
		config:  config,
	}
	return cluster, nil
}

func (c *Cluster) WaitForLeader(timeout time.Duration) (*raftnode.Node, error) {
	quoromAgrees := func(n *raftnode.Node) bool {
		addr, _ := n.Raft.LeaderWithID()
		if addr == "" {
			return false
		}
		aggree := 0
		sz := 0
		for _, node := range c.nodes {
			if node.Raft.State() != raft.Follower {
				continue
			}
			sz++
			leaderAddr, _ := node.Raft.LeaderWithID()
			if leaderAddr == addr {
				aggree++
			}
		}

		if sz == 0 {
			return false
		}

		return aggree >= sz/2+1
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	timer := time.NewTimer(timeout)
	defer ticker.Stop()
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			for _, n := range c.nodes {
				if n.Raft.State() == raft.Leader {
					if quoromAgrees(n) {
						return n, nil
					}
				}
			}
		case <-timer.C:
			return nil, fmt.Errorf("no leader found under %v", timeout)
		}
	}
}

func (c *Cluster) Shutdown(timeout time.Duration) {
	go func() {
		for _, n := range c.nodes {
			n.Shutdown(timeout)
		}
	}()

	go func() {
		for _, s := range c.servers {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			s.Shutdown(ctx)
		}
	}()
}
