package mock

import (
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
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
		Storage:   config.InmemStorage,
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

func NewMockNodeEnv(tb testing.TB, config *config.Config, raftConfig *raft.Config, logger *slog.Logger, fsm store.FSMStore) *raftnode.NodeEnv {
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

func NewMockFSMStore() store.FSMStore {
	return &MockFSMStore{}
}

// MockFSMStore is an implementation of the FSM interface, and just stores
// the logs sequentially.
// It also implements store.KVStore so its compatible with raftnode.Node
type MockFSMStore struct {
	sync.Mutex
	logs [][]byte
}

// LogsUnsafe returns the logs of the fsm WITHOUT locking it
func (m *MockFSMStore) LogsUnsafe() [][]byte {
	// m.Lock()
	// defer m.Unlock()
	return m.logs
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

// Apply implements raft.FSM
func (m *MockFSMStore) Apply(log *raft.Log) interface{} {
	m.Lock()
	defer m.Unlock()
	m.logs = append(m.logs, log.Data)
	return len(m.logs)
}

// Snapshot implements raft.FSM
func (m *MockFSMStore) Snapshot() (raft.FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

// Restore implements raft.FSM
func (m *MockFSMStore) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer func() { _ = inp.Close() }()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockFSMStore) Set(key string, value []byte) error { return nil }

// Mutation -> through raft
func (m *MockFSMStore) Delete(key string) (value []byte, err error) { return nil, nil }

// Query -> local read, may be stale (since it doesnt go through raft)
func (m *MockFSMStore) GetStale(key string) (value []byte, err error) { return nil, nil }

// Persist implements raft.FSMSnapshot
func (m *MockSnapshot) Persist(sink raft.SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.logs[:m.maxIndex]); err != nil {
		_ = sink.Cancel()
		return err
	}
	_ = sink.Close()
	return nil
}

// Release implements raft.FSMSnapshot
func (m *MockSnapshot) Release() {
}
