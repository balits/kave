package node

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage/backend"
	transport "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
)

type Node struct {
	bootstrap      bool
	logger         *slog.Logger
	cfg            *config.Config
	fsm            *fsm.Fsm
	raftDeps       *config.RaftDependencies // stored bcs raft.HasExistingState() upon Bootstrap requires it
	raft           *raft.Raft
	kvService      service.KVService
	peerService    service.PeerService
	clusterService service.ClusterService
	httpServer     *transport.HttpServer
}

func New(cfg *config.Config, logger *slog.Logger) (*Node, error) {
	backend := backend.NewBackend(cfg.StorageOpts)
	kvstore := mvcc.NewKVStore(logger, backend)
	fsm := fsm.NewFsm(logger, kvstore, cfg.Me.NodeID)

	hclogger := util.NewHcLogAdapter(logger, cfg.LogLevel)
	raftCfg := config.NewRaftConfig(cfg.Me.NodeID, hclogger, cfg.LogLevel)
	raftDeps, err := config.NewRaftDependencies(cfg.Me.GetRaftAddress(), cfg.StorageOpts.Dir, hclogger)
	if err != nil {
		return nil, err
	}

	n := &Node{
		bootstrap: cfg.Bootstrap,
		logger:    logger.With("component", "node"),
		cfg:       cfg,
		fsm:       fsm,
		raftDeps:  raftDeps,
	}

	r, err := raft.NewRaft(raftCfg, n.fsm, raftDeps.LogStore, raftDeps.StableStore, raftDeps.SnapshotStore, raftDeps.Transport)
	if err != nil {
		return nil, fmt.Errorf("failed to setup raft: %v", err)
	}

	n.raft = r
	n.peerService = service.NewPeerService(n.raft, cfg)
	n.kvService = service.NewKVService(logger, n.raft, kvstore, n.peerService)
	n.clusterService = service.NewClusterService(n.raft, cfg, logger)
	n.httpServer = transport.NewHTTPServer(cfg.Me.GetInternalHttpAddress(), n.kvService, n.clusterService, n.peerService, cfg, logger)
	return n, nil
}

func (n *Node) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// bootstrap, restore state or block on joining the cluster
	err := n.BootstrapOrJoin(ctx)
	if err != nil {
		n.logger.Error("Failed to bootstrap or join cluster", "error", err)
		return err
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(n.httpServer.Start)

	// shutdown watcher
	g.Go(func() error {
		<-ctx.Done()
		return n.Shutdown(context.Background())
	})

	// wait for error
	if err := g.Wait(); err != nil {
		n.logger.Error("Node stopped with error", "error", err)
		return err
	}

	return nil
}

func (n *Node) BootstrapOrJoin(ctx context.Context) error {
	hasState, err := raft.HasExistingState(
		n.raftDeps.LogStore,
		n.raftDeps.StableStore,
		n.raftDeps.SnapshotStore,
	)

	if err != nil {
		n.logger.Error("Failed to read raft state", "error", err)
		return err
	}

	if hasState {
		n.logger.Info("Existing Raft state found; resuming cluster participation")
		// raft will recover terms, logs etc
		return nil
	}

	if n.bootstrap {
		n.logger.Info("Bootstrapping new cluster")
		return n.clusterService.Bootstrap(ctx)
	}

	timeout := 5 * time.Second
	n.logger.Info("No existing Raft state found; waiting for cluster to be bootstrapped", "timeout", timeout)
	<-time.After(timeout)
	n.logger.Info("Joining existing cluster")
	return n.clusterService.JoinCluster(ctx, n.peerService.GetPeers())
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.logger.Info("Shutdown initiated")
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 1) stop traffic
	if err := n.httpServer.Shutdown(timeout); err != nil {
		return err
	}

	// 2) stop raft
	if err := n.raft.Shutdown().Error(); err != nil {
		return err
	}

	n.logger.Info("Shutdown completed")
	return nil
}

// testing helpers

// func (n *Node) Cfg() *config.Config {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}
// 	return n.cfg
// }

// func (n *Node) GetLeader() (string, error) {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}

// 	leaderAddr := n.raft.Leader()
// 	if leaderAddr == "" {
// 		return "", fmt.Errorf("no leader found")
// 	}
// 	return string(leaderAddr), nil
// }

// func (n *Node) IsLeader() bool {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}

// 	return n.raft.State() == raft.Leader
// }

// func (n *Node) GetPeers() ([]raft.ServerAddress, error) {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}

// 	servers := n.raft.GetConfiguration().Configuration().Servers
// 	peers := make([]raft.ServerAddress, len(servers))
// 	for i, srv := range servers {
// 		peers[i] = srv.Address
// 	}
// 	return peers, nil
// }

// func (n *Node) Get(ctx context.Context, key string) (string, error) {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}

// 	if err := n.raft.VerifyLeader().Error(); err != nil {
// 		return "", common.ErrNotLeader
// 	}

// 	waitForCatchUp(ctx, n.raft)

// 	resp, err := n.kvService.Get(ctx, common.GetRequest{Key: []byte(key)})
// 	if err != nil {
// 		return "", err
// 	}
// 	return string(resp.Value), nil
// }

// func (n *Node) Set(ctx context.Context, key, value string) error {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}

// 	if err := n.raft.VerifyLeader().Error(); err != nil {
// 		return common.ErrNotLeader
// 	}

// 	_, err := n.kvService.Set(ctx, common.SetRequest{Key: []byte(key), Value: []byte(value)})
// 	return err
// }

// // not used yet
// func waitForCatchUp(ctx context.Context, r *raft.Raft) {
// 	if !testing.Testing() {
// 		panic("testing helpers can only be used in go tests")
// 	}

// 	for {
// 		// check if there are any logs that need to be applied
// 		if r.AppliedIndex() >= r.LastIndex() {
// 			return
// 		}

// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-time.After(100 * time.Millisecond):
// 			continue
// 		}
// 	}
// }
