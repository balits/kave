package service

import (
	"context"
	"fmt"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/common"
	"github.com/hashicorp/raft"
)

type PeerService interface {
	Me() common.Peer
	GetPeers() map[string]common.Peer
	GetLeader() (common.Peer, error)
	VerifyLeader(ctx context.Context) error
}

func NewPeerService(raft *raft.Raft, cfg *config.Config) PeerService {
	peers := make(map[string]common.Peer)
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
	peers map[string]common.Peer
	me    common.Peer
}

func (p *peerService) Me() common.Peer {
	return p.me
}

func (p *peerService) GetPeers() map[string]common.Peer {
	m := make(map[string]common.Peer)
	for id, peer := range p.peers {
		if p.me.NodeID == id {
			continue
		}
		m[id] = peer
	}
	return m
}

func (p *peerService) GetLeader() (common.Peer, error) {
	leaderAddr, leaderID := p.r.LeaderWithID()
	fmt.Println("GetLeader leaderID:", leaderID, "leaderAddr:", leaderAddr, "peers:", p.peers)
	if string(leaderID) == "" {
		return common.Peer{}, fmt.Errorf("%w: %v", common.ErrLeaderNotFound, "empty leaderID")
	}

	leader, ok := p.peers[string(leaderID)]
	if !ok {
		return common.Peer{}, fmt.Errorf("%w: %v", common.ErrLeaderNotFound, "leader not in peer map")
	}
	return leader, nil
}

func (p *peerService) VerifyLeader(ctx context.Context) error {
	return waitFuture(ctx, p.r.VerifyLeader())
}
