// package node implements a single raft node with an http server and a key-value store
package raftnode

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/store"
	"github.com/hashicorp/raft"
)

type Node struct {
	// Raft is the pointer to Raft instance
	Raft *raft.Raft

	// Store that implements raft.FSM and store.KVStore
	Store store.FSMStore

	// Logger provides structured logging
	Logger *slog.Logger

	// Config is the cluster wide configuration
	Config *config.Config

	// RaftStores is a reference to all the storage raft.Raft needs (logstore, stablestore, snapshot store)
	// it is redundant to carry this in our struct, however it's vital for checking a clusters existing state through raft.HasState(...)
	// and sadly but understandably raft.Raft does not expose these fields
	RaftStores *RaftStores

	// joinedState is a flag that signals if the node is part of the cluster or not
	joinedState atomic.Bool
}

// NewNode creates a node without any raft functionality
// This is part one of the two phase initialization
func NewNode(config *config.Config, fsm store.FSMStore, stores *RaftStores, logger *slog.Logger) (*Node, error) {
	r, err := SetupRaft(config, fsm, stores)
	if err != nil {
		logger.Error("Failed to setup raft", "error", err)
		return nil, err
	}

	node := &Node{
		Raft:        r,
		Store:       fsm,
		Logger:      logger,
		Config:      config,
		RaftStores:  stores,
		joinedState: atomic.Bool{},
	}

	return node, nil
}

// SetupRaft creates a Raft instance based on a config
func SetupRaft(config *config.Config, store store.FSMStore, stores *RaftStores) (*raft.Raft, error) {
	raftConfig := loadRaftConfig(config.ThisService.RaftID, config.LogLevel)
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.ThisService.GetRaftAddress())
	if err != nil {
		return nil, fmt.Errorf("couldn't resolve address: %w", err)
	}

	transport, err := raft.NewTCPTransport(tcpAddr.String(), tcpAddr, len(config.ClusterInfo), 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("couldn't build tcp transport: %w", err)
	}

	raftNode, err := raft.NewRaft(raftConfig, store, stores.LogStore, stores.StableStore, stores.SnapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("couldn't create raft instance: %w", err)
	}

	return raftNode, nil
}

// Shutdown terminates both the http server with the supplied timeout and the raft node
func (n *Node) Shutdown(timeout time.Duration) {
	if n.Raft != nil {
		n.joinedState.Store(false)
		if err := n.Raft.Shutdown().Error(); err != nil {
			n.Logger.Info("Failed to shut down Raft node: %v", "error", err)
		}
		n.Logger.Info("Shut down Raft node successfuly")
	}
}

func (n *Node) PartOfCluster() bool {
	return n.joinedState.Load()
}

func loadRaftConfig(nodeID string, logLevel string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	cfg.LogLevel = logLevel
	return cfg
}
