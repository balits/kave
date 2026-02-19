package testx

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/fsm"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/store/inmem"
	"github.com/balits/thesis/internal/testx/mock"
	"github.com/hashicorp/raft"
)

func NewTestNode(tb testing.TB, ctx context.Context, nodeID string) *TestNode {
	loglevel := GetTestingLogLevel()
	config := mock.NewMockConfig(tb, loglevel, true)
	config.NodeID = nodeID
	config.LogLevel = loglevel
	logger := NewTestLogger(tb, config.LogLevel)
	raftConfig := mock.NewMockRaftConfig(logger, config.LogLevel)
	raftConfig.LocalID = raft.ServerID(nodeID)
	fsm := fsm.New(mock.NewLoggingStore(inmem.NewStore()))
	env := mock.NewMockNodeEnv(tb, config, raftConfig, logger, fsm)
	node, server := mock.NewMockNode(tb, env)

	n := &TestNode{node, fsm, server}
	n.StartHttpServer(tb, ctx) // TODO: make this optional
	return n
}

type TestNode struct {
	*raftnode.Node
	LoggingFsm *fsm.FSM // exposed access to fsm which should stay private in prod
	server     *api.Server
}

func (node *TestNode) Release() {
	_ = node.Shutdown(time.Millisecond * 200)
	os.RemoveAll(node.Dir)
}

// Restart will start a raft node that was previously Shutdown()
func (node *TestNode) Restart(tb testing.TB) {
	addr := string(node.Transport.LocalAddr())
	transport, err := raft.NewTCPTransport(addr, nil, 2, time.Second, nil)
	if err != nil {
		tb.Fatalf("err: %v", err)
	}
	node.Transport = transport
	node.Config.RaftPort = strings.Split(string(transport.LocalAddr()), ":")[1]
	node.Logger.Info("starting node", "addr", transport.LocalAddr())
	node.Raft = nil
	err = node.SetupRaft()
	if err != nil {
		tb.Fatalf("err: %v", err)
	}
}

func (node *TestNode) StartHttpServer(tb testing.TB, ctx context.Context) {
	go func() {
		if err := node.server.Run(ctx); err != nil && err != http.ErrServerClosed {
			tb.Logf("HTTP server error: %v", err)
		}
	}()
}

func (node *TestNode) Bootstrap() error {
	hasState, err := raft.HasExistingState(node.Logs, node.Stable, node.Snapshots)
	if err != nil {
		return fmt.Errorf("failed reading raft state: %v", err)
	}

	if hasState {
		node.Logger.Info("Exsisting Raft state found; resuming cluster participation")
	}

	if !hasState && node.Config.Bootstrap {
		if err := node.BootstrapCluster(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
		node.Logger.Info("Bootstrapped cluster successfuly")
	}
	return nil
}
