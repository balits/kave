package service

import (
	"context"
	"fmt"

	"github.com/balits/kave/internal/config"
	"github.com/hashicorp/raft"
)

var ErrLeaderNotFound = fmt.Errorf("leader not found")

type PeerService interface {
	Me() config.Peer
	GetPeers() map[string]config.Peer
	GetLeader() (config.Peer, error)
	VerifyLeader(ctx context.Context) error
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
	_, leaderID := p.r.LeaderWithID()
	if string(leaderID) == "" {
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
