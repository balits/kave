package peer

import (
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
)

// Peer represents information about a node in the cluster
type Peer struct {
	NodeID   string `json:"node_id"`   // raft server id
	Hostname string `json:"hostname"`  // optional hostname: in test we use localhost, in prod leave it empty, so we can use the advertised hostname (NodeID)
	RaftPort string `json:"raft_port"` // raft port of the node
	HttpPort string `json:"http_port"` // http port of the node
}

func (p *Peer) String() string {
	return fmt.Sprintf("%s:%s:%s:%s", p.NodeID, p.Hostname, p.RaftPort, p.HttpPort)
}

func (p *Peer) GetRaftAddress() raft.ServerAddress {
	if p.Hostname == "" {
		return raft.ServerAddress(p.NodeID + ":" + p.RaftPort)
	}
	return raft.ServerAddress(p.Hostname + ":" + p.RaftPort)
}

func (p *Peer) GetHttpAdvertisedAddress() string {
	if p.Hostname == "" {
		return p.NodeID + ":" + p.HttpPort
	}
	return p.Hostname + ":" + p.HttpPort
}

func (p *Peer) GetRaftListenAddress() string {
	return "0.0.0.0:" + p.RaftPort
}

func (p *Peer) GetHttpListenAddress() string {
	return "0.0.0.0:" + p.HttpPort
}

func (p *Peer) HttpScheme() string {
	return "http"
}

// "http://" + p.GetHttpAddress()
func (p *Peer) HttpURL() string {
	return p.HttpScheme() + "://" + p.GetHttpAdvertisedAddress()
}

func (p *Peer) Check() error {
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
