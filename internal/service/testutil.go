package service

import (
	"context"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/transport"
	"github.com/hashicorp/raft"
)

type MockPeerService struct {
	Me_             config.Peer
	Leader          config.Peer
	ErrLeader       error
	ErrVerifyLeader error
	State_          raft.RaftState
	ErrLag          error
}

func (p *MockPeerService) Me() config.Peer                      { return p.Me_ }
func (p *MockPeerService) State() raft.RaftState                { return p.State_ }
func (p *MockPeerService) GetPeers() map[string]config.Peer     { return nil }
func (p *MockPeerService) GetLeader() (config.Peer, error)      { return p.Leader, p.ErrLeader }
func (p *MockPeerService) VerifyLeader(_ context.Context) error { return p.ErrVerifyLeader }
func (p *MockPeerService) LaggingBehind() error                 { return p.ErrLag }

type MockClusterService struct{}

func (n *MockClusterService) Bootstrap(_ context.Context) error { return nil }
func (n *MockClusterService) JoinCluster(_ context.Context, _ map[string]config.Peer) error {
	return nil
}
func (n *MockClusterService) AddToCluster(_ context.Context, _ transport.JoinRequest) error {
	return nil
}
func (n *MockClusterService) Stats() (map[string]string, error) {
	return map[string]string{"state": "Leader"}, nil
}
