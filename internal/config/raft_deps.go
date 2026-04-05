package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/balits/kave/internal/util/logutil"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftDependencies struct {
	LogStore      raft.LogStore
	SnapshotStore raft.SnapshotStore
	StableStore   raft.StableStore
	Transport     raft.Transport
	Logger        *logutil.HcLogAdapter
}

func NewRaftConfig(nodeID string, logger *logutil.HcLogAdapter, logLevel slog.Level) *raft.Config {
	// tweak settings for easier testing
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(nodeID)
	rc.LogLevel = logLevel.String()
	rc.Logger = logger

	if testing.Testing() {
		rc.SnapshotInterval = time.Second * 5
		rc.SnapshotThreshold = 5
	}

	return rc
}

func NewRaftDependencies(advertiseAddr raft.ServerAddress, listenAddr string, dir string, logger *logutil.HcLogAdapter) (*RaftDependencies, error) {
	var (
		logs     raft.LogStore
		stable   raft.StableStore
		snapshot raft.SnapshotStore
		err      error
	)

	if testing.Testing() {
		logs = raft.NewInmemStore()
		stable = raft.NewInmemStore()
		snapshot = raft.NewInmemSnapshotStore()
	} else {
		logstorePath := filepath.Join(dir, "logstore.db")
		logs, err = raftboltdb.NewBoltStore(logstorePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create logstore: %v", err)
		}

		stablePath := filepath.Join(dir, "stablestore.db")
		stable, err = raftboltdb.NewBoltStore(stablePath)
		if err != nil {
			return nil, fmt.Errorf("couldn't create raft stable store: %w", err)
		}

		snapshot, err = raft.NewFileSnapshotStoreWithLogger(dir, 10, logger)
		if err != nil {
			return nil, fmt.Errorf("couldn't create raft snapshot store: %w", err)
		}
	}

	tcpAdvertiseAddr, err := net.ResolveTCPAddr("tcp", string(advertiseAddr))
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	tr, err := raft.NewTCPTransport(listenAddr, tcpAdvertiseAddr, 2, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport(listen = %s, adverstise = %s): %v", listenAddr, advertiseAddr, err)
	}

	deps := &RaftDependencies{
		LogStore:      logs,
		StableStore:   stable,
		SnapshotStore: snapshot,
		Transport:     tr,
	}

	return deps, nil
}
