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
	"github.com/hashicorp/raft"
)

type Node struct {
	Raft   *raft.Raft
	Store  store.FSMStore
	Logger *slog.Logger
	Config *config.Config
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
		Raft:   r,
		Store:  fsm,
		Logger: logger,
		Config: config,
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

func (n *Node) BootstrapOrJoinCluster() error {
	raftStores, err := LoadRaftStores(n.Config)
	if err != nil {
		return fmt.Errorf("failed loading raft stores: %v", err)
	}
	hasState, err := raft.HasExistingState(raftStores.LogStore, raftStores.StableStore, raftStores.SnapshotStore)
	if err != nil {
		return fmt.Errorf("failed reading raft state: %v", err)
	}

	if hasState {
		n.Logger.Info("Exsisting Raft state found; resuming cluster participation")
		// Raft will recover terms, logs etc
		return nil
	}

	if n.Config.ThisService.NeedBootstrap {
		if err := n.bootstrap(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
		n.Logger.Info("Bootstrapped cluster successfuly")
	} else {
		if err := n.joinCluster(); err != nil {
			return fmt.Errorf("failed to join cluster: %v", err)
		}
		n.Logger.Info("Joined cluster successfuly")
	}

	return nil
}

// bootstrap tries to
func (n *Node) bootstrap() error {
	// with only this node in the configuration (no peers)
	// bootstrapping the cluster will immediatly elect this node to leader,
	// without wasting time with heartbeats to other nodes
	clusterConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.Config.ThisService.RaftID),
				Address: raft.ServerAddress(n.Config.ThisService.GetRaftAddress()),
			},
		},
	}
	return n.Raft.BootstrapCluster(clusterConfig).Error()
}

func (n *Node) joinCluster() error {
	me := n.Config.ThisService
	var urls []string
	for _, i := range n.Config.ClusterInfo {
		if i == *me {
			continue
		}
		urls = append(urls, "http://"+i.RaftHost+":"+i.InternalHttpPort+"/join")
	}
	n.Logger.Debug("Attempting to join cluster", "peers", urls)
	if err := Join(me, urls, 4); err != nil {
		n.Logger.Error("Joining cluster failed", "error", err)
		return err
	}
	return nil
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

func loadRaftConfig(nodeID string, logLevel string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	cfg.LogLevel = logLevel
	return cfg
}
