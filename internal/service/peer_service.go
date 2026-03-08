package service

import (
	"context"
	"fmt"

	"github.com/balits/kave/internal/config"
	"github.com/hashicorp/raft"
)

var ErrLeaderNotFound = fmt.Errorf("leader not found")

const ApplyLagReadinessThreshold = 10

type PeerService interface {
	// Information about ourselves
	Me() config.Peer
	// Our current state
	State() raft.RaftState
	// All other peers in the cluster
	GetPeers() map[string]config.Peer
	// The current leader, if any
	GetLeader() (config.Peer, error)
	// Verify that we are the leader, or return an error if not
	VerifyLeader(ctx context.Context) error
	// Check if we are lagging behind the leader, and return an error if we are
	LaggingBehind() error
}

func NewPeerService(raft *raft.Raft, cfg *config.Config) PeerService {
	peers := make(map[string]config.Peer)
	for _, peer := range cfg.Peers {
		peers[peer.NodeID] = peer
	}

	return &peerService{
		r:     raft,
		peers: peers,
		me:    cfg.Me,
	}
}

type peerService struct {
	r     *raft.Raft
	peers map[string]config.Peer
	me    config.Peer
}

func (p *peerService) Me() config.Peer {
	return p.me
}

func (p *peerService) State() raft.RaftState {
	return p.r.State()
}

func (p *peerService) GetPeers() map[string]config.Peer {
	m := make(map[string]config.Peer)
	for id, peer := range p.peers {
		if p.me.NodeID == id {
			continue
		}
		m[id] = peer
	}
	return m
}

func (p *peerService) GetLeader() (config.Peer, error) {
	leaderAddr, leaderID := p.r.LeaderWithID()
	if string(leaderID) == "" || string(leaderAddr) == "" {
		return config.Peer{}, fmt.Errorf("%w: %v", ErrLeaderNotFound, "empty leaderID")
	}

	leader, ok := p.peers[string(leaderID)]
	if !ok {
		return config.Peer{}, fmt.Errorf("%w: %v", ErrLeaderNotFound, "leader not in peer map")
	}
	return leader, nil
}

func (p *peerService) VerifyLeader(ctx context.Context) error {
	return waitFuture(ctx, p.r.VerifyLeader())
}

func (p *peerService) LaggingBehind() error {
	c, a := p.r.CommitIndex(), p.r.AppliedIndex()
	if c-a > ApplyLagReadinessThreshold {
		return fmt.Errorf("raft is lagging behind: commit index %d, applied index %d", c, a)
	}
	return nil
}
