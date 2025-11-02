// package node implements a single raft node with an http server and a key-value store
package raftnode

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/util"
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
}

// NewNode creates a node without any raft functionality
// This is part one of the two phase initialization
func NewNode(config *config.Config, fsm store.FSMStore, stores *RaftStores, logger *slog.Logger) (*Node, error) {
	nodeLogger := logger.With("component", "node")
	raftLogger := logger.With("component", "raftlib")

	raftConfig := loadRaftConfig(config.ThisService.RaftID, config.LogLevel, raftLogger)
	r, err := SetupRaft(config, raftConfig, fsm, stores)
	if err != nil {
		nodeLogger.Error("Failed to setup raft", "error", err)
		return nil, err
	}

	node := &Node{
		Raft:       r,
		Store:      fsm,
		Logger:     nodeLogger,
		Config:     config,
		RaftStores: stores,
	}

	return node, nil
}

// SetupRaft creates a Raft instance based on a config
func SetupRaft(config *config.Config, raftConfig *raft.Config, store store.FSMStore, stores *RaftStores) (*raft.Raft, error) {
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
		if err := n.Raft.Shutdown().Error(); err != nil {
			n.Logger.Info("Failed to shut down Raft node: %v", "error", err)
		}
		n.Logger.Info("Shut down Raft node successfuly")
	}
}

func loadRaftConfig(nodeID string, logLevel string, logger *slog.Logger) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	cfg.LogLevel = logLevel
	cfg.Logger = util.NewHcLogAdapter(logger, util.StringToSlogLevel(logLevel))
	return cfg
}
