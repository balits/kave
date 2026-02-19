package raftnode

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/fsm"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/store/durable"
	"github.com/balits/thesis/internal/store/inmem"
	"github.com/balits/thesis/internal/util"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type NodeEnv struct {
	Dir    string
	fsm    *fsm.FSM
	Logger *slog.Logger
	Config *config.Config

	RaftConfig *raft.Config
	Transport  raft.Transport
	Logs       raft.LogStore
	Stable     raft.StableStore
	Snapshots  raft.SnapshotStore
}

// GetStore returns the
func (e *NodeEnv) GetStore() store.Storage {
	return e.fsm.Store
}

// SetFsm helps us to set the (not exported) fsm field in tests
func (e *NodeEnv) SetFsm(fsm *fsm.FSM) {
	if !testing.Testing() {
		panic("SetFSM can only be called in tests")
	}
	e.fsm = fsm
}

func NewEnv(cfg *config.Config, logger *slog.Logger) (*NodeEnv, error) {
	fsm, err := newFsm(cfg.Storage, cfg.Dir)
	if err != nil {
		return nil, err
	}

	raftCfg := loadRaftConfig(raft.ServerID(cfg.NodeID), logger, cfg.LogLevel)
	transport, err := newTransport(cfg.GetRaftAddress())
	if err != nil {
		return nil, err
	}

	logs, stable, snapshots, err := newRaftStores(cfg.Storage, cfg.Dir)
	if err != nil {
		return nil, err
	}

	env := &NodeEnv{
		Dir:        cfg.Dir,
		fsm:        fsm,
		Logger:     logger.With("component", "node"),
		Config:     cfg,
		RaftConfig: raftCfg,
		Transport:  transport,
		Logs:       logs,
		Snapshots:  snapshots,
		Stable:     stable,
	}

	return env, nil
}

func newFsm(kind config.StorageKind, dataDir string) (*fsm.FSM, error) {
	var s store.Storage
	switch kind {
	case config.StorageKindBolt:
		durable, err := durable.NewStore(dataDir + "/kvbolt.db")
		if err != nil {
			return nil, err
		}
		s = durable
	case config.StorageKindInMemory:
		s = inmem.NewStore()
	}

	return fsm.New(s), nil
}

func newRaftStores(storage config.StorageKind, dir string) (logs raft.LogStore, stable raft.StableStore, snapshots raft.SnapshotStore, err error) {
	if storage == config.StorageKindInMemory {
		logs = raft.NewInmemStore()
		stable = raft.NewInmemStore()
		snapshots = raft.NewInmemSnapshotStore()
		return
	}

	logPath := path.Join(dir, "raft-log.db")
	logs, err = raftboltdb.NewBoltStore(logPath)
	if err != nil {
		err = fmt.Errorf("couldn't create raft log store: %w", err)
		return
	}

	stablePath := path.Join(dir, "raft-stable.db")
	stable, err = raftboltdb.NewBoltStore(stablePath)
	if err != nil {
		err = fmt.Errorf("couldn't create raft stable store: %w", err)
		return
	}

	snapshots, err = raft.NewFileSnapshotStore(dir, 10, os.Stderr)
	if err != nil {
		err = fmt.Errorf("couldn't create raft snapshot store: %w", err)
		return
	}
	return
}

func newTransport(addr raft.ServerAddress) (raft.Transport, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", string(addr))
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}

	transport, err := raft.NewTCPTransport(tcpAddr.String(), tcpAddr, 2, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("couldn't build tcp transport: %w", err)
	}

	return transport, nil
}

func loadRaftConfig(nodeID raft.ServerID, logger *slog.Logger, level slog.Level) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = nodeID
	cfg.LogLevel = level.String()
	cfg.Logger = util.NewHcLogAdapter(logger.With("component", "raftlib"), level)
	// cfg.ElectionTimeout = 50 * time.Millisecond
	// cfg.ElectionTimeout = 50 * time.Millisecond
	// cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	// cfg.CommitTimeout = 5 * time.Millisecond
	return cfg
}
