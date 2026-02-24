package common

import (
	"fmt"
	"strings"

	"github.com/hashicorp/raft"
)

// Peer represents information about a node in the cluster
type Peer struct {
	NodeID         string `json:"node_id"`          // raft server id
	Hostname       string `json:"-"`                // optional hostname: in test we use localhost, in prod leave it empty, so we can use the advertised hostname (NodeID)
	RaftPort       string `json:"raft_port"`        // raft port of the node
	HttpPort       string `json:"http_port"`        // http port of the node
	PublicHttpHost string `json:"public_http_host"` //
}

func (p *Peer) GetRaftAddress() raft.ServerAddress {
	if p.Hostname == "" {
		return raft.ServerAddress(p.NodeID + ":" + p.RaftPort)
	}
	return raft.ServerAddress(p.Hostname + ":" + p.RaftPort)
}

func (p *Peer) GetInternalHttpAddress() string {
	return p.NodeID + ":" + p.HttpPort
}

func (p *Peer) GetPublicHttpAddress() string {
	if p.PublicHttpHost != "" {
		return p.PublicHttpHost + ":" + p.HttpPort
	}
	return p.GetInternalHttpAddress()
}

func (p *Peer) ValidateNodeConfig() error {
	if strings.TrimSpace(p.NodeID) == "" || strings.TrimSpace(p.RaftPort) == "" || strings.TrimSpace(p.HttpPort) == "" {
		return fmt.Errorf("failed to parse peer: field missing or invalid (expected id:raft_port:http_port)")
	}
	return nil
}

func (p Peer) String() string {
	return fmt.Sprintf("Peer{NodeID: %s, Hostname: %s, RaftPort: %s, HttpPort: %s, PublicHttpHost: %s}", p.NodeID, p.Hostname, p.RaftPort, p.HttpPort, p.PublicHttpHost)
}
