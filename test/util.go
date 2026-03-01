package test

import (
	"log/slog"
	"os"
	"testing"

	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/inmem"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
)

const BucketTest storage.Bucket = "test_bucket"

func LogLevel() slog.Level {
	if testing.Verbose() {
		return slog.LevelDebug
	} else {
		return slog.LevelInfo
	}
}

func NewTestLogger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: LogLevel(),
	}))
}

func NewTestRaft(t testing.TB) *raft.Raft {
	return NewTestRaftWithStore(t, inmem.NewStore(storage.StorageOptions{
		InitialBuckets: []storage.Bucket{
			BucketTest,
		},
	}))
}

func NewTestRaftWithStore(t testing.TB, st storage.Storage) *raft.Raft {
	id := raft.ServerID("dummy")
	_f := fsm.New(st)

	conf := raft.DefaultConfig()
	conf.Logger = util.NewHcLogAdapter(NewTestLogger(t), LogLevel())
	conf.LocalID = id
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(""))
	node, err := raft.NewRaft(conf, _f, logs, stable, snaps, trans)
	if err != nil {
		t.Errorf("could not create raft node: %v", err)
	}

	f := node.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      id,
				Address: trans.LocalAddr(),
			},
		},
	})

	if err = f.Error(); err != nil {
		t.Errorf("could not bootstrap raft node: %v", err)
	}
	// time.Sleep(2 * time.Second)
	<-node.LeaderCh() // wait until node becomes leader (pre voting, voting election etc)

	return node
}
