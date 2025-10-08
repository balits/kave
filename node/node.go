// package implements a single raft node with an http server and a key-value store
package node

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/balits/thesis/config"
	"github.com/balits/thesis/store"
	"github.com/balits/thesis/web"
	"github.com/hashicorp/raft"
)

type Node struct {
	raft   *raft.Raft
	store  store.KVStore
	fsm    *FSM
	server *web.Server
	logger *slog.Logger
}

func NewNode(confing *config.NodeConfig, logger *slog.Logger, server *web.Server) (*Node, error) {
	var (
		addr   string = config.Config.RaftAddr
		data   string = config.Config.DataDir
		inmem  bool   = config.Config.Inmem
		nodeID string = config.Config.NodeID
	)

	cfg := loadRaftConfig(nodeID)
	kvstore := store.NewInMemoryStore()
	fsm := NewFSM(kvstore)

	raft, err := setupRaft(cfg, fsm, addr, inmem, data)
	if err != nil {
		return nil, err
	}

	node := &Node{
		raft:   raft,
		store:  kvstore,
		fsm:    fsm,
		server: server,
		logger: logger,
	}

	node.logger.Debug("Node initialized", "nodeID", nodeID, "address", addr)

	return node, nil
}

func setupRaft(config *raft.Config, fsm *FSM, raftAddr string, inmem bool, datadir string) (*raft.Raft, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("couldnt resolve address: %v", err)
	}

	transport, err := raft.NewTCPTransport(tcpAddr.String(), tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("couldnt build tcp transport: %v", err)
	}

	var (
		boltStore     raft.LogStore
		stableStore   raft.StableStore
		snapshotStore raft.SnapshotStore
	)

	if inmem {
		boltStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapshotStore = raft.NewInmemSnapshotStore()
	} else {
		return nil, fmt.Errorf("only in-memory storage is supported")
		// boltStore, err = raftboltdb.NewBoltStore(path.Join(datadir, "bolt"))
		// if err != nil {
		// 	return nil, fmt.Errorf("couldnt create raft  store: %v", err)
		// }

		// snapshotStore, err = raft.NewFileSnapshotStore(path.Join(datadir, "snapshot"), 10, os.Stderr)
		// if err != nil {
		// 	return nil, fmt.Errorf("couldnt create raft snapshot store: %v", err)
		// }
	}

	dirpath := "data"
	err = os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("couldnt create raft storage directory: %v", err)
	}

	raftNode, err := raft.NewRaft(config, fsm, boltStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("couldnt create raft instance: %v", err)
	}

	hasState, err := raft.HasExistingState(boltStore, stableStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("couldnt check existing state: %v", err)
	}

	if !hasState {
		raftNode.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: raft.ServerAddress(raftAddr),
				},
			},
		})
	}

	return raftNode, nil
}

func loadRaftConfig(nodeID string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	cfg.HeartbeatTimeout = 1000 * time.Millisecond
	return cfg
}
