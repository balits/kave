package mock

import (
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/fsm"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// NewMockConfig creates a config for testing, leaving out peer information about us and the whole cluster
// which should be set up later
func NewMockConfig(tb testing.TB, level slog.Level, bootstrap bool) *config.Config {
	dir, err := os.MkdirTemp("", "raft")
	if err != nil {
		tb.Fatalf("failed to create tempdir: %v", err)
	}

	c := &config.Config{
		Bootstrap: bootstrap,
		Dir:       dir,
		Storage:   config.StorageKindInMemory,
		LogLevel:  level,
		Peers:     make([]config.Peer, 0),
	}

	return c
}

func NewMockRaftConfig(logger *slog.Logger, level slog.Level) *raft.Config {
	if testing.Verbose() {
		level = slog.LevelInfo
	}
	cfg := raft.DefaultConfig()
	cfg.LogLevel = level.String()
	// cfg.Logger = util.NewHcLogAdapter(logger.With("component", "raftlib"), level)
	cfg.Logger = nil
	cfg.LogLevel = hclog.Off.String()
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.SnapshotThreshold = 100
	cfg.TrailingLogs = 10
	return cfg
}

func NewMockNodeEnv(tb testing.TB, config *config.Config, raftConfig *raft.Config, logger *slog.Logger, fsm *fsm.FSM) *raftnode.NodeEnv {
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snapshots := raft.NewInmemSnapshotStore()

	transport, err := raft.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	if err != nil {
		tb.Fatalf("failed to create transport: %v", err)
	}

	c := &raftnode.NodeEnv{
		Dir:        config.Dir,
		Logger:     logger.With("component", "node"),
		Config:     config,
		RaftConfig: raftConfig,
		Transport:  transport,
		Logs:       logs,
		Snapshots:  snapshots,
		Stable:     stable,
	}
	c.SetFsm(fsm)
	return c
}

func NewMockServer(tb testing.TB, config *config.Config, node *raftnode.Node, logger *slog.Logger) *api.Server {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("failed to create server: %v", err)
	}
	addr := ln.Addr()
	if err = ln.Close(); err != nil {
		tb.Fatalf("failed to close listener: %v", err)
	}
	return api.NewServer(addr.String(), node, logger)
}

func NewMockNode(tb testing.TB, env *raftnode.NodeEnv) (*raftnode.Node, *api.Server) {
	node := raftnode.CreateNode(env)
	if err := node.SetupRaft(); err != nil {
		tb.Fatal(err)
	}
	server := NewMockServer(tb, env.Config, node, env.Logger)

	raftPort := strings.Split(string(env.Transport.LocalAddr()), ":")[1]
	if raftPort == "" {
		tb.Fatalf("raft port empty (%s)", raftPort)
	}
	httpPort := strings.Split(server.Addr, ":")[1]
	if httpPort == "" {
		tb.Fatalf("http port empty (%s)", httpPort)
	}
	p := config.Peer{
		NodeID:   env.Config.NodeID, // already set
		RaftPort: raftPort,
		HttpPort: httpPort,
		Hostname: "127.0.0.1", // set for tests, so GetRaftAddress returns a valid address (not just NodeID + raftPort)
	}
	env.Config.Peer = p

	return node, server
}
