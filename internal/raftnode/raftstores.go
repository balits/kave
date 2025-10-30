package raftnode

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/balits/thesis/internal/config"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

var (
	storesOnce sync.Once
	stores     *RaftStores
	storesErr  error
)

type RaftStores struct {
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
}

func LoadRaftStores(config *config.Config) (*RaftStores, error) {
	storesOnce.Do(func() {
		var err error
		stores, err = NewRaftStores(config.DataDir, config.ThisService.RaftID, config.InMemory)
		if err != nil {
			storesErr = fmt.Errorf("failed to load raft stores: %v", err)
		}
	})

	return stores, storesErr
}

// NewRaftStores creates raft storage structs (log, stable, snapshot) either on disk or in memory
func NewRaftStores(dir, raftId string, inMemory bool) (*RaftStores, error) {
	var (
		logStore      raft.LogStore
		stableStore   raft.StableStore
		snapshotStore raft.SnapshotStore
		err           error
	)

	nodeSpecificDataDir := filepath.Join(dir, raftId)
	if err = os.MkdirAll(nodeSpecificDataDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("couldn't create raft storage directory: %w", err)
	}

	if inMemory {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapshotStore = raft.NewInmemSnapshotStore()
	} else {
		logStorePath := path.Join(nodeSpecificDataDir, "raft-log.db")
		if logStore, err = raftboltdb.NewBoltStore(logStorePath); err != nil {
			return nil, fmt.Errorf("couldn't create raft log store: %w", err)
		}

		stableStorePath := path.Join(nodeSpecificDataDir, "raft-stable.db")
		if stableStore, err = raftboltdb.NewBoltStore(stableStorePath); err != nil {
			return nil, fmt.Errorf("couldn't create raft stable store: %w", err)
		}

		if snapshotStore, err = raft.NewFileSnapshotStore(nodeSpecificDataDir, 10, os.Stderr); err != nil {
			return nil, fmt.Errorf("couldn't create raft snapshot store: %w", err)
		}
	}

	return &RaftStores{
		LogStore:      logStore,
		StableStore:   stableStore,
		SnapshotStore: snapshotStore,
	}, nil
}
