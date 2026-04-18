package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/compaction"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/util"
	"github.com/balits/kave/internal/util/logutil"
	"github.com/balits/kave/internal/watch"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

// Used to determine if the apply index lags behind
// the commit index.
// TODO: make this configurable or just leave it as is?
const APPLY_LAG_THRESHOLD = 20

type Node struct {
	Bootstrap  bool
	Logger     *slog.Logger
	Me         peer.Peer
	IsShutdown bool // usefull for tests

	ProposeFunc  util.ProposeFunc
	IsLeaderFunc util.IsLeaderFunc

	Backend      backend.Backend
	KvIndex      kv.Index
	WatchHub     *watch.WatchHub
	UnsyncedLoop *watch.UnsyncedLoop
	KvStore      *mvcc.KvStore
	LeaseManager *lease.LeaseManager
	OtManager    *ot.OTManager
	Engine       *mvcc.Engine
	Fsm          *fsm.Fsm
	Raft         *raft.Raft
	RaftDeps     *config.RaftDependencies // stored bcs raft.HasExistingState() upon Bootstrap requires it

	// services
	DiscoveryService peer.DiscoveryService
	KvService        service.KVService
	LeaseService     service.LeaseService
	OtService        service.OTService
	RaftService      service.RaftService

	// transport
	HttpServer *http.HttpServer

	// background routines
	RaftObserver        *raft.Observer
	RaftEventWatcher    *fsm.RaftEventWatcher
	CompactionScheduler *compaction.CompactionScheduler
	CheckpointScheduler *lease.CheckpointScheduler
	ExpiryLoop          *lease.ExpiryLoop
}

func New(cfg *config.Config, logger *slog.Logger, reg *prometheus.Registry) (*Node, error) {
	s, _ := json.MarshalIndent(cfg, "", "    ")
	fmt.Printf("creating node with config:\n%s\n", string(s))

	n := &Node{
		Me:        cfg.Me,
		Bootstrap: cfg.Bootstrap,
		Logger:    logger.With("node_id", cfg.Me.NodeID),
	}

	if err := n.initStorage(reg, cfg.StorageOpts, cfg.OtOpts); err != nil {
		return nil, fmt.Errorf("failed to setup storage: %v", err)
	}

	if err := n.initRaft(reg, cfg); err != nil {
		return nil, fmt.Errorf("failed to setup raft: %v", err)
	}

	n.ProposeFunc = util.NewProposeFunc(n.Raft)
	n.IsLeaderFunc = util.NewIsLeaderFunc(n.Raft, raft.ServerID(cfg.Me.NodeID))

	if err := n.initBackgroundRoutines(cfg.CheckpointIntervalMinutes, &cfg.CompactionOpts); err != nil {
		return nil, fmt.Errorf("failed to setup background processes: %v", err)
	}

	if err := n.initServices(cfg); err != nil {
		return nil, fmt.Errorf("failed to setup services: %v", err)
	}

	if err := n.registerObservers(); err != nil {
		return nil, fmt.Errorf("failed to register observers: %v", err)
	}

	n.HttpServer = http.NewHTTPServer(
		n.Logger,
		cfg.Me,
		n.DiscoveryService,
		n.KvService,
		n.LeaseService,
		n.OtService,
		n.RaftService,
		n.WatchHub,
		reg,
		cfg.RatelimiterOpts.Read,
		cfg.RatelimiterOpts.Write,
	)
	return n, nil
}

func (n *Node) Run(parentCtx context.Context) error {
	runCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	me := n.DiscoveryService.Me()
	peerList, err := n.DiscoveryService.GetPeers(runCtx)
	if err != nil {
		n.Logger.Error("Failed to start cluster: peer discovery failed", "error", err)
		return fmt.Errorf("peer discovery failed: %w", err)
	}

	err = n.RaftService.RegisterPeers(peerList)
	// this normally shouldnt happen, but check it still
	if err != nil {
		n.Logger.Error("Failed to start cluster: peer registration failed", "error", err)
		return fmt.Errorf("peer registration failed: %w", err)
	}

	g, groupCtx := errgroup.WithContext(runCtx)

	g.Go(n.HttpServer.Start)

	g.Go(func() error {
		n.CompactionScheduler.Run(groupCtx)
		return nil
	})

	g.Go(func() error {
		n.ExpiryLoop.Run(groupCtx)
		return nil
	})

	g.Go(func() error {
		n.CheckpointScheduler.Run(groupCtx)
		return nil
	})

	g.Go(func() error {
		return n.RaftEventWatcher.Run(groupCtx)
	})

	g.Go(func() error {
		n.UnsyncedLoop.Run(groupCtx)
		return nil
	})

	// shutdown watcher
	g.Go(func() error {
		<-runCtx.Done()
		n.Logger.Info("Run context canceled, shuting down node")
		return n.Shutdown(context.Background())
	})

	// bootstrap, restore state or block on joining the cluster
	err = n.BootstrapOrJoin(runCtx, me)
	if err != nil {
		n.Logger.Error("Failed to bootstrap or join cluster", "error", err)
		return err
	}

	// wait for error on background routines
	if err := g.Wait(); err != nil {
		n.Logger.Error("Node stopped with error", "error", err)
		return err
	}

	return nil
}

func (n *Node) BootstrapOrJoin(ctx context.Context, me peer.Peer) error {
	hasState, err := raft.HasExistingState(
		n.RaftDeps.LogStore,
		n.RaftDeps.StableStore,
		n.RaftDeps.SnapshotStore,
	)

	if err != nil {
		n.Logger.Error("Failed to read raft state", "error", err)
		return err
	}

	if hasState {
		n.Logger.Info("Existing Raft state found; resuming cluster participation")
		// raft will recover terms, logs etc
		return nil
	}

	if n.Bootstrap {
		if err := n.bootstrap(ctx, me); err != nil {
			return fmt.Errorf("bootstrap failed: %w", err)
		}
	}

	n.Logger.Info("No existing Raft state found; joining existing cluster")
	return n.RaftService.JoinCluster(ctx, me)
}

func (n *Node) bootstrap(ctx context.Context, me peer.Peer) error {
	n.Logger.Info("Bootstrapping new cluster")
	if err := n.RaftService.Bootstrap(ctx, me); err != nil {
		n.Logger.Error("Failed to read bootstrap cluster", "error", err)
		return err
	}

	// busy loop polling is not the most elegant
	// but for a one time key gen its okay
	// we used Leadership event watchers in other,
	// more important places
	keyGenCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	waitUntilLeaderC := make(chan error)

	go func() {
		for {
			select {
			case <-keyGenCtx.Done():
				waitUntilLeaderC <- keyGenCtx.Err()
				return
			default:
			}

			if n.RaftService.RaftState() == raft.Leader {
				waitUntilLeaderC <- nil
				return
			}
		}
	}()
	err := <-waitUntilLeaderC
	if err != nil {
		return err
	}

	_, err = n.ProposeFunc(ctx, command.Command{Kind: command.KindOTGenerateClusterKey})
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.Logger.Info("Shutdown initiated")
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 0.1) deregister raft observer
	n.Raft.DeregisterObserver(n.RaftObserver)
	n.RaftEventWatcher.Stop()

	// 0.2) stop background routines
	n.CompactionScheduler.Stop()
	n.CheckpointScheduler.Stop()
	n.ExpiryLoop.Stop()

	// 1) stop traffic
	if err := n.HttpServer.Shutdown(timeout); err != nil {
		return err
	}

	// 2) stop raft
	if err := n.Raft.Shutdown().Error(); err != nil {
		return err
	}

	n.Logger.Info("Shutdown completed")
	n.IsShutdown = true
	return nil
}

func (n *Node) initStorage(reg prometheus.Registerer, storageOpts storage.Options, otOpts ot.Options) error {
	n.Backend = backend.New(reg, n.Logger, storageOpts)
	n.KvIndex = kv.NewTreeIndex(n.Logger)
	n.KvStore = mvcc.NewKvStoreWithIndex(reg, n.Logger, n.Backend, n.KvIndex)
	n.LeaseManager = lease.NewManager(reg, n.Logger, n.KvStore, n.Backend)
	om, err := ot.NewOTManager(reg, n.Logger, n.Backend, otOpts)
	if err != nil {
		return err
	}
	// we need to read the ClusterKey from the backend,
	// by this no node in cluster would have generated it
	// if err := om.InitTokenCodec(); err != nil {
	// 	return err
	// }
	n.OtManager = om

	n.WatchHub = watch.NewWatchHub(reg, n.Logger, n.KvStore)
	n.UnsyncedLoop = watch.NewUnsyncedLoop(n.Logger, n.WatchHub, n.KvIndex, n.Backend.ReadTx(), n.KvStore)
	n.Engine = mvcc.NewEngine(n.KvStore, n.LeaseManager, n.WatchHub)

	return nil
}

func (n *Node) initRaft(reg prometheus.Registerer, cfg *config.Config) error {
	n.Fsm = fsm.NewWithEngine(n.Logger, cfg.Me, n.KvStore, n.LeaseManager, n.OtManager, n.Engine)

	slogLevel := cfg.LoggerOptions.Level.ToSlogLevel()
	// override raft.Config logging options (raft.Config is still injectible, just not the logger fields)
	cfg.RaftCfg.LogLevel = slogLevel.String()
	cfg.RaftCfg.Logger = logutil.NewHcLogAdapter(n.Logger.With("component", "raftlib"), slogLevel)

	raftDeps, err := config.NewRaftDependencies(cfg.Me.GetRaftAddress(), cfg.Me.GetRaftListenAddress(), cfg.StorageOpts.Dir, cfg.RaftCfg.Logger)
	if err != nil {
		return err
	}
	n.RaftDeps = raftDeps

	r, err := raft.NewRaft(cfg.RaftCfg, n.Fsm, raftDeps.LogStore, raftDeps.StableStore, raftDeps.SnapshotStore, raftDeps.Transport)
	if err != nil {
		return err
	}
	n.Raft = r

	raftMetrics := metrics.NewRaftMetrics(reg, r, config.ApplyLagReadinessThreshold)
	n.Fsm.SetMetrics(raftMetrics)
	c := make(chan raft.Observation, 64) // buffer, so leadership events are not dropped before watcher starts
	n.RaftEventWatcher = fsm.NewRaftEventWatcher(n.Logger, c, raftMetrics, raft.ServerID(cfg.Me.NodeID))
	n.RaftObserver = raft.NewObserver(c, false, n.RaftEventWatcher.FilterFn())

	return nil
}

// registering the raft observer happens here, so that our background routines have the most up to date info
func (n *Node) initBackgroundRoutines(intervalMinutes time.Duration, opts *compaction.Options) error {
	n.CheckpointScheduler = lease.NewCheckpointScheduler(n.Logger, n.LeaseManager, intervalMinutes*time.Minute, n.ProposeFunc, n.IsLeaderFunc)
	n.ExpiryLoop = lease.NewExpiryLoop(n.Logger, n.LeaseManager, n.ProposeFunc, n.IsLeaderFunc)
	n.CompactionScheduler = compaction.NewScheduler(n.Logger, n.KvStore, n.ProposeFunc, n.IsLeaderFunc, opts)
	return nil
}

func (n *Node) initServices(cfg *config.Config) error {
	discoveryService, err := peer.NewDiscoveryService(cfg.Me, cfg.PodNamespace, cfg.PeerDiscoveryOptions)
	if err != nil {
		return err
	}
	n.DiscoveryService = discoveryService
	n.RaftService = service.NewRaftService(n.Logger, n.Raft, APPLY_LAG_THRESHOLD)
	n.LeaseService = service.NewLeaseService(n.Logger, n.ProposeFunc)
	n.OtService = service.NewOTService(n.Logger, cfg.Me, n.KvStore, n.OtManager, n.RaftService, n.ProposeFunc)
	n.KvService = service.NewKVService(n.Logger, cfg.Me, n.KvStore, n.RaftService, cfg.KvOptions, n.ProposeFunc)
	return nil
}

func (n *Node) registerObservers() error {
	n.Fsm.RegisterObservers(n.CompactionScheduler)
	n.RaftEventWatcher.RegisterLeadershipObservers(n.CheckpointScheduler, n.ExpiryLoop, n.CompactionScheduler)
	n.Raft.RegisterObserver(n.RaftObserver)
	return nil
}
