package service

import (
	"context"
	"errors"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
)

type mockPeerService struct {
	me       config.Peer
	isLeader util.IsLeaderFunc
}

func (p *mockPeerService) Me() config.Peer { return p.me }
func (p *mockPeerService) State() raft.RaftState {
	if p.isLeader() {
		return raft.Leader
	} else {
		return raft.Follower
	}
}

func (p *mockPeerService) GetPeers() map[string]config.Peer { return make(map[string]config.Peer) }

func (p *mockPeerService) GetLeader() (config.Peer, error) {
	if p.isLeader() {
		return p.me, nil
	} else {
		return config.Peer{}, nil
	}
}

func (p *mockPeerService) VerifyLeader(ctx context.Context) error {
	if p.isLeader() {
		return nil
	} else {
		return errors.New("not leader")
	}
}

func (p *mockPeerService) LaggingBehind() error {
	return nil
}

type mockApplyFuture struct {
	result any
	index  uint64
}

func (f *mockApplyFuture) Error() error {
	return nil
}

func (f *mockApplyFuture) Index() uint64 {
	return f.index
}

func (f *mockApplyFuture) Response() any {
	return f.result
}
