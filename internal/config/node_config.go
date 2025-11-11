package config

import (
	"fmt"
	"strings"

	"github.com/hashicorp/raft"
)

// Peer represents information about a node in the cluster
type Peer struct {
	NodeID   string `json:"node_id"`   // raft server id
	Hostname string `json:"-"`         // optional hostname: in test we use localhost, in prod leave it empty, so we can use the advertised hostname (NodeID)
	RaftPort string `json:"raft_port"` // raft port of the node
	HttpPort string `json:"http_port"` // http port of the node
}

func (c *Peer) GetRaftAddress() raft.ServerAddress {
	if c.Hostname == "" {
		return raft.ServerAddress(c.NodeID + ":" + c.RaftPort)
	}
	return raft.ServerAddress(c.Hostname + ":" + c.RaftPort)
}

func (c *Peer) GetInternalHttpAddress() string {
	return c.NodeID + ":" + c.HttpPort
}

func (c *Peer) ValidateNodeConfig() error {
	if strings.TrimSpace(c.NodeID) == "" || strings.TrimSpace(c.RaftPort) == "" || strings.TrimSpace(c.HttpPort) == "" {
		return fmt.Errorf("failed to parse peer: field missing or invalid (expected id:raft_port:http_port)")
	}
	return nil
}
