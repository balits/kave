package node

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/compaction"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	transport "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/util"
	"github.com/balits/kave/internal/util/logutil"
	"github.com/balits/kave/internal/watch"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type Node struct {
	bootstrap bool
	logger    *slog.Logger

	proposeFunc  util.ProposeFunc
	isLeaderFunc util.IsLeaderFunc

	backend      backend.Backend
	kvIndex      kv.Index
	hub          *watch.WatchHub
	unsyncedLoop *watch.UnsyncedLoop
	kvstore      *mvcc.KvStore
	leaseManager *lease.LeaseManager
	otManager    *ot.OTManager
	engine       *mvcc.Engine
	fsm          *fsm.Fsm
	raft         *raft.Raft
	raftDeps     *config.RaftDependencies // stored bcs raft.HasExistingState() upon Bootstrap requires it

	// services
	kvService      service.KVService
	leaseService   service.LeaseService
	otService      service.OTService
	peerService    service.PeerService
	clusterService service.ClusterService

	// transport
	httpServer *transport.HttpServer

	// background routines
	observer            *raft.Observer
	raftEventWatcher    *fsm.RaftEventWatcher
	compactionScheduler *compaction.CompactionScheduler
	checkpointScheduler *lease.CheckpointScheduler
	expiryLoop          *lease.ExpiryLoop
}

func New(cfg *config.Config, logger *slog.Logger, reg *prometheus.Registry) (*Node, error) {
	n := &Node{
		bootstrap: cfg.Bootstrap,
		logger:    logger.With("node_id", cfg.Me.NodeID),
	}

	if err := n.initStorage(reg, cfg.StorageOpts, cfg.OtOpts); err != nil {
		return nil, fmt.Errorf("failed to setup storage: %v", err)
	}

	if err := n.initRaft(reg, cfg); err != nil {
		return nil, fmt.Errorf("failed to setup raft: %v", err)
	}

	n.proposeFunc = util.NewProposeFunc(n.raft)
	n.isLeaderFunc = util.NewIsLeaderFunc(n.raft, raft.ServerID(cfg.Me.NodeID))

	if err := n.initBackgroundRoutines(cfg.CheckpointIntervalMinutes, &cfg.CompactionOpts); err != nil {
		return nil, fmt.Errorf("failed to setup background processes: %v", err)
	}

	if err := n.initServices(cfg); err != nil {
		return nil, fmt.Errorf("failed to setup services: %v", err)
	}

	if err := n.registerObservers(); err != nil {
		return nil, fmt.Errorf("failed to register observers: %v", err)
	}

	n.httpServer = transport.NewHTTPServer(
		logger,
		cfg.Me.GetHttpAdvertisedAddress(),
		n.kvService,
		n.leaseService,
		n.otService,
		n.clusterService,
		n.peerService,
		n.hub,
		reg,
		// TODO(ratelimiter): run benches to figure out real R and B
		transport.NewRateLimiterConfig(1000, 200),
		transport.NewRateLimiterConfig(100, 20),
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
		n.compactionScheduler.Run(ctx)
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

	// 0.1) deregister raft observer
	n.raft.DeregisterObserver(n.observer)
	n.raftEventWatcher.Stop()

	// 0.2) stop background routines
	n.compactionScheduler.Stop()
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

func (n *Node) initStorage(reg prometheus.Registerer, storageOpts storage.StorageOptions, otOpts ot.Options) error {
	n.backend = backend.New(reg, storageOpts)
	n.kvIndex = kv.NewTreeIndex(n.logger)
	n.kvstore = mvcc.NewKvStoreWithIndex(reg, n.logger, n.backend, n.kvIndex)
	om, err := ot.NewOTManager(reg, n.logger, n.backend, otOpts)
	if err != nil {
		return err
	}
	n.otManager = om

	n.hub = watch.NewWatchHub(reg, n.logger, n.kvstore)
	n.unsyncedLoop = watch.NewUnsyncedLoop(n.logger, n.hub, n.kvIndex, n.backend.ReadTx(), n.kvstore)
	n.engine = mvcc.NewEngine(n.kvstore, n.leaseManager, n.hub)

	return nil
}

func (n *Node) initRaft(reg prometheus.Registerer, cfg *config.Config) error {
	n.leaseManager = lease.NewManager(reg, n.logger, n.kvstore, n.backend)
	n.fsm = fsm.NewWithEngine(n.logger, n.kvstore, n.leaseManager, n.otManager, n.engine, cfg.Me.NodeID)

	hclogger := logutil.NewHcLogAdapter(n.logger, cfg.LogLevel)
	raftCfg := config.NewRaftConfig(cfg.Me.NodeID, hclogger, cfg.LogLevel)
	raftDeps, err := config.NewRaftDependencies(cfg.Me.GetRaftAddress(), cfg.Me.GetRaftListenAddress(), cfg.StorageOpts.Dir, hclogger)
	if err != nil {
		return err
	}
	n.raftDeps = raftDeps

	r, err := raft.NewRaft(raftCfg, n.fsm, raftDeps.LogStore, raftDeps.StableStore, raftDeps.SnapshotStore, raftDeps.Transport)
	if err != nil {
		return err
	}
	n.raft = r

	raftMetrics := metrics.NewRaftMetrics(reg, r, config.ApplyLagReadinessThreshold)
	n.fsm.SetMetrics(raftMetrics)
	c := make(chan raft.Observation)
	n.raftEventWatcher = fsm.NewRaftEventWatcher(n.logger, c, raftMetrics, raft.ServerID(cfg.Me.NodeID))
	n.observer = raft.NewObserver(c, false, n.raftEventWatcher.FilterFn())

	return nil
}

// registering the raft observer happens here, so that our background routines have the most up to date info
func (n *Node) initBackgroundRoutines(intervalMinutes time.Duration, opts *compaction.CompactionOptions) error {
	n.checkpointScheduler = lease.NewCheckpointScheduler(n.logger, n.leaseManager, intervalMinutes*time.Minute, n.proposeFunc, n.isLeaderFunc)
	n.expiryLoop = lease.NewExpiryLoop(n.logger, n.leaseManager, n.proposeFunc, n.isLeaderFunc)
	n.compactionScheduler = compaction.NewScheduler(n.logger, n.kvstore, n.proposeFunc, n.isLeaderFunc, opts)
	return nil
}

func (n *Node) initServices(cfg *config.Config) error {
	n.peerService = service.NewPeerService(n.raft, cfg)
	n.leaseService = service.NewLeaseService(n.logger, n.proposeFunc)
	n.otService = service.NewOTService(n.logger, n.kvstore, n.otManager, n.peerService, n.proposeFunc)
	n.kvService = service.NewKVService(n.logger, n.kvstore, n.peerService, n.proposeFunc)
	n.clusterService = service.NewClusterService(n.raft, cfg, n.logger)
	return nil
}

func (n *Node) registerObservers() error {
	n.fsm.RegisterObservers(n.compactionScheduler)
	n.raftEventWatcher.RegisterLeadershipObservers(n.checkpointScheduler, n.expiryLoop, n.compactionScheduler)
	n.raft.RegisterObserver(n.observer)
	return nil
}
