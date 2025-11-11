package testx

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/hashicorp/raft"
)

type Cluster struct {
	nodes        map[raft.ServerAddress]*TestNode // nodes in the cluster
	tb           testing.TB                       // internal testing handle
	dns          map[config.Peer]bool             // for service discovery
	bootstrapped bool                             // if the cluster has been bootstrapped or not
}

func NewCluster(tb testing.TB, ctx context.Context) *Cluster {
	return &Cluster{
		nodes: make(map[raft.ServerAddress]*TestNode),
		tb:    tb,
		dns:   make(map[config.Peer]bool),
	}
}

/*
	func NewClusterWithSize(tb testing.TB, ctx context.Context, size int) *Cluster {
		switch size {
		case 1, 3, 5:
		default:
			tb.Fatalf("only odd cluster sizes of 1, 3, 5 are supported, got %d", size)
		}

		cluster := &Cluster{
			Nodes: make(map[raft.ServerAddress]*TestNode),
			tb:    tb,
		}

		// bootstrap the first node
		node1 := CreateAndRunNode(tb, ctx, "node1", true)
		check(tb, waitForState(node1, raft.Leader))
		cluster.Add(node1)

		// create the rest
		for i := 1; i < size; i++ {
			id := fmt.Sprintf("node%d", i+1)
			n := CreateAndRunNode(tb, ctx, id, false)
			// addr := n.Transport.LocalAddr()
			// check(tb, waitForFuture(tb, node1.Raft.AddVoter(raft.ServerID(id), addr, 0, 0)))
			cluster.Add(n)
			cluster.Join(n.Config.GetRaftAddress())
		}

		return cluster
	}
*/
func (c *Cluster) Bootstrap(node *TestNode) error {
	if c.bootstrapped {
		return fmt.Errorf("cluster already bootstrapped")
	}
	if _, ok := c.nodes[node.Config.GetRaftAddress()]; ok {
		return fmt.Errorf("node %s already part of cluster", node.Config.GetRaftAddress())
	}
	c.bootstrapped = true
	c.add(node)
	return node.Bootstrap()
}

// add adds a node to the cluster's internal map and DNS
// this is for internal use
// USE JOIN OR BOOTSTRAP INSTEAD
func (c *Cluster) add(node *TestNode) {
	c.nodes[node.Config.GetRaftAddress()] = node
	c.dns[node.Config.Peer] = true
}

func (c *Cluster) Join(node *TestNode) error {
	nodeAddr := node.Config.GetRaftAddress()
	_, ok := c.nodes[nodeAddr]
	if !ok {
		c.add(node)
	} else {
		return fmt.Errorf("node %s already part of cluster", nodeAddr)
	}

	var urls []string
	for peer, ok := range c.dns {
		if !ok || node.Config.Peer == peer {
			continue
		}
		u := "http://localhost:" + peer.HttpPort + "/join"
		urls = append(urls, u)
	}

	attempts := 1
	timeout := time.Duration(math.Pow(float64(attempts), float64(2))+1) * time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	joinCh := make(chan error, 1)
	go func() {
		err := raftnode.JoinWithBackoff(node.Config.Peer, urls, attempts, node.Logger)
		joinCh <- err
	}()

	select {
	case err := <-joinCh:
		return err
	case <-timer.C:
		return NewErrTimeout(timeout, "Join")
	}
}

func (c *Cluster) Remove(node *TestNode) {
	delete(c.nodes, node.Config.GetRaftAddress())
	delete(c.dns, node.Config.Peer)
}

func (c *Cluster) Slice() []*TestNode {
	nodes := make([]*TestNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (c *Cluster) Shutdown() {
	for _, n := range c.nodes {
		n.Release()
		delete(c.dns, n.Config.Peer)
	}
	c.bootstrapped = false
}
