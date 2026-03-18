package node

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/compactor"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	transport "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/util"
	"github.com/balits/kave/internal/util/logutil"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type Node struct {
	bootstrap bool
	logger    *slog.Logger

	backend backend.Backend
	kvstore *mvcc.KVStore

	fsm      *fsm.Fsm
	raft     *raft.Raft
	raftDeps *config.RaftDependencies // stored bcs raft.HasExistingState() upon Bootstrap requires it

	leaseMgr *lease.LeaseManager

	// services
	kvService      service.KVService
	leaseService   service.LeaseService
	peerService    service.PeerService
	clusterService service.ClusterService

	// transport
	httpServer *transport.HttpServer

	// background processes
	observer            *raft.Observer
	raftEventWatcher    *fsm.RaftEventWatcher
	compactor           compactor.Compactor
	checkpointScheduler *lease.CheckpointScheduler
	expiryLoop          *lease.ExpiryLoop
}

func New(cfg *config.Config, logger *slog.Logger, reg *prometheus.Registry) (*Node, error) {
	n := &Node{
		bootstrap: cfg.Bootstrap,
		logger:    logger.With("node_id", cfg.Me.NodeID),
	}

	if err := n.initStorage(reg, cfg.StorageOpts); err != nil {
		return nil, fmt.Errorf("failed to setup storage: %v", err)
	}

	if err := n.initRaft(reg, cfg); err != nil {
		return nil, fmt.Errorf("failed to setup raft: %v", err)
	}

	proposeFunc, isLeaderFunc := util.NewProposeFunc(n.raft), util.NewIsLeaderFunc(n.raft, cfg.Me)

	if err := n.initBackgroundProcesses(cfg.CheckpointInterval, cfg.CompactorOpts, proposeFunc, isLeaderFunc); err != nil {
		return nil, fmt.Errorf("failed to setup background processes: %v", err)
	}

	if err := n.initServices(cfg, proposeFunc); err != nil {
		return nil, fmt.Errorf("failed to setup services: %v", err)
	}

	n.httpServer = transport.NewHTTPServer(
		logger,
		cfg.Me.HttpPort,
		n.kvService,
		n.leaseService,
		n.clusterService,
		n.peerService,
		cfg,
		reg,
	)
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

	g.Go(func() error {
		n.compactor.Run(ctx)
		return nil
	})

	g.Go(func() error {
		n.expiryLoop.Run(ctx)
		return nil
	})

	g.Go(func() error {
		n.checkpointScheduler.Run(ctx)
		return nil
	})

	g.Go(func() error {
		n.raftEventWatcher.Run()
		return nil
	})

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

	// 0.1) deregister observer
	n.raft.DeregisterObserver(n.observer)
	n.raftEventWatcher.Stop()

	// 0.2) stop compactor
	n.compactor.Stop()
	n.checkpointScheduler.Stop()
	n.expiryLoop.Stop()

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

func (n *Node) initStorage(reg prometheus.Registerer, opts storage.StorageOptions) error {
	n.backend = backend.NewBackend(reg, opts)
	n.kvstore = mvcc.NewKVStore(reg, n.logger, n.backend)
	return nil
}

func (n *Node) initRaft(reg prometheus.Registerer, cfg *config.Config) error {
	n.leaseMgr = lease.NewLeaseManager(reg, n.logger, n.kvstore, n.backend)
	n.fsm = fsm.NewFsm(n.logger, n.kvstore, n.leaseMgr, cfg.Me.NodeID)

	hclogger := logutil.NewHcLogAdapter(n.logger, cfg.LogLevel)
	raftCfg := config.NewRaftConfig(cfg.Me.NodeID, hclogger, cfg.LogLevel)
	raftDeps, err := config.NewRaftDependencies(cfg.Me.GetRaftAddress(), cfg.StorageOpts.Dir, hclogger)
	if err != nil {
		return err
	}
	n.raftDeps = raftDeps

	// TODO: move CompactorOptions fields to config
	n.compactor = compactor.New(n.logger, n.kvstore, compactor.CompactorOptions{
		Kind:         compactor.CompactorWindowRetention,
		Threshold:    20,
		WindowSize:   10,
		PollInterval: 10 * time.Second,
	})

	r, err := raft.NewRaft(raftCfg, n.fsm, raftDeps.LogStore, raftDeps.StableStore, raftDeps.SnapshotStore, raftDeps.Transport)
	if err != nil {
		return err
	}
	n.raft = r

	raftMetrics := metrics.NewRaftMetrics(reg, r, config.ApplyLagReadinessThreshold)
	n.fsm.SetMetrics(raftMetrics)
	c := make(chan raft.Observation)
	n.raftEventWatcher = fsm.NewRaftEventWatcher(c, raftMetrics, raft.ServerID(cfg.Me.NodeID))
	n.observer = raft.NewObserver(c, false, n.raftEventWatcher.FilterFn())

	return nil
}

// registering the raft observer happens here, so that our background routines have the most up to date info
func (n *Node) initBackgroundProcesses(interval time.Duration, opts compactor.CompactorOptions, propose util.ProposeFunc, isLeader util.IsLeaderFunc) error {
	n.checkpointScheduler = lease.NewCheckpointScheduler(n.logger, n.leaseMgr, interval, propose, isLeader)
	n.expiryLoop = lease.NewExpiryLoop(n.logger, n.leaseMgr, propose, isLeader)
	n.compactor = compactor.New(n.logger, n.kvstore, opts)

	n.raftEventWatcher.RegisterLeadershipObservers(n.checkpointScheduler, n.expiryLoop)
	n.raft.RegisterObserver(n.observer)

	return nil
}

func (n *Node) initServices(cfg *config.Config, propose util.ProposeFunc) error {
	n.peerService = service.NewPeerService(n.raft, cfg)
	n.leaseService = service.NewLeaseService(n.logger, propose)
	n.kvService = service.NewKVService(n.logger, n.kvstore, n.peerService, propose)
	n.clusterService = service.NewClusterService(n.raft, cfg, n.logger)
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
