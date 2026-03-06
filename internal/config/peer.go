package config

import (
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
)

// Peer represents information about a node in the cluster
type Peer struct {
	NodeID   string `json:"node_id"`   // raft server id
	Hostname string `json:"-"`         // optional hostname: in test we use localhost, in prod leave it empty, so we can use the advertised hostname (NodeID)
	RaftPort string `json:"raft_port"` // raft port of the node
	HttpPort string `json:"http_port"` // http port of the node
}

func (p *Peer) GetRaftAddress() raft.ServerAddress {
	if p.Hostname == "" {
		return raft.ServerAddress(p.NodeID + ":" + p.RaftPort)
	}
	return raft.ServerAddress(p.Hostname + ":" + p.RaftPort)
}

func (p *Peer) GetHttpAddress() string {
	return p.NodeID + ":" + p.HttpPort
}


func (p *Peer) validateNodeConfig() error {
	if p.NodeID == "" {
		return errors.New("node ID is required")
	}
	if p.RaftPort == "" {
		return errors.New("raft port is required")
	}
	if p.HttpPort == "" {
		return errors.New("http port is required")
	}
	return nil
}

func (p Peer) String() string {
	return fmt.Sprintf("Peer{NodeID: %s, Hostname: %s, RaftPort: %s, HttpPort: %s}", p.NodeID, p.Hostname, p.RaftPort, p.HttpPort)
}
