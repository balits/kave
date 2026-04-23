package service

import (
	"context"

	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/transport"
	"github.com/hashicorp/raft"
)

type MockRaftService struct {
	Me_             peer.Peer
	Leader_         peer.Peer
	ErrLeader       error
	ErrVerifyLeader error
	State_          raft.RaftState
	ErrLag          error
}

func (m *MockRaftService) RegisterPeers(_ []peer.Peer) error                             { return nil }
func (m *MockRaftService) Bootstrap(_ context.Context, _ peer.Peer) error                { return nil }
func (m *MockRaftService) JoinCluster(_ context.Context, _ peer.Peer, _ string) error    { return nil }
func (m *MockRaftService) AddToCluster(_ context.Context, _ transport.JoinRequest) error { return nil }
func (m *MockRaftService) Peers() []peer.Peer                                            { return nil }
func (m *MockRaftService) Leader(_ context.Context) (peer.Peer, error)                   { return m.Leader_, m.ErrLeader }
func (m *MockRaftService) RaftConfiguration(_ context.Context) (c raft.Configuration, e error) {
	return
}
func (m *MockRaftService) Stats(_ context.Context) map[string]string {
	return map[string]string{"state": "Leader"}
}
func (m *MockRaftService) RaftState() raft.RaftState            { return m.State_ }
func (m *MockRaftService) VerifyLeader(_ context.Context) error { return m.ErrVerifyLeader }
func (m *MockRaftService) LaggingBehind() error                 { return m.ErrLag }
