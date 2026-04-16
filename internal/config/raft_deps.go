package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftDependencies struct {
	LogStore      raft.LogStore
	SnapshotStore raft.SnapshotStore
	StableStore   raft.StableStore
	Transport     raft.Transport
	HcLogger      hclog.Logger
}

func NewDefaultRaftConfig(nodeID string) *raft.Config {
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(nodeID)
	return rc
}

func NewRaftDependencies(advertiseAddr raft.ServerAddress, listenAddr string, dir string, logger hclog.Logger) (*RaftDependencies, error) {
	var (
		logs      raft.LogStore
		stable    raft.StableStore
		snapshot  raft.SnapshotStore
		transport raft.Transport
		err       error
	)

	if testing.Testing() {
		logs = raft.NewInmemStore()
		stable = raft.NewInmemStore()
		snapshot = raft.NewInmemSnapshotStore()
		_, transport = raft.NewInmemTransport(advertiseAddr)
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

		tcpAdvertiseAddr, err := net.ResolveTCPAddr("tcp", string(advertiseAddr))
		if err != nil {
			return nil, fmt.Errorf("failed to create transport: %v", err)
		}

		transport, err = raft.NewTCPTransport(listenAddr, tcpAdvertiseAddr, 2, 10*time.Second, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("failed to create transport(listen = %s, adverstise = %s): %v", listenAddr, advertiseAddr, err)
		}
	}

	deps := &RaftDependencies{
		LogStore:      logs,
		StableStore:   stable,
		SnapshotStore: snapshot,
		Transport:     transport,
		HcLogger:      logger,
	}

	return deps, nil
}
