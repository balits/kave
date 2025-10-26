// package node implements a single raft node with an http server and a key-value store
package service

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/balits/thesis/pkg/config"
	"github.com/balits/thesis/pkg/store"
	"github.com/balits/thesis/pkg/web"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Service struct {
	Raft   *raft.Raft
	Store  store.KVStore
	FSM    *FSM
	Server *web.Server
	Logger *slog.Logger
	Config *config.Config
}

// NewService creates a node without any raft functionality
// This is part one of the two phase initialization
func NewService(store store.KVStore, fsm *FSM, server *web.Server, logger *slog.Logger, config *config.Config) *Service {
	return &Service{
		Raft:   nil,
		Store:  store,
		FSM:    fsm,
		Server: server,
		Logger: logger,
		Config: config,
	}
}

// NewRaft creates a Raft instance based on our service
func (s *Service) NewRaft() (*raft.Raft, error) {
	raftConfig := loadRaftConfig(s.Config.ThisService.RaftID, s.Config.LogLevel)
	tcpAddr, err := net.ResolveTCPAddr("tcp", s.Config.ThisService.GetRaftAddress())
	if err != nil {
		return nil, fmt.Errorf("couldn't resolve address: %w", err)
	}

	transport, err := raft.NewTCPTransport(tcpAddr.String(), tcpAddr, len(s.Config.ClusterInfo), 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("couldn't build tcp transport: %w", err)
	}

	var (
		logStore      raft.LogStore
		stableStore   raft.StableStore
		snapshotStore raft.SnapshotStore
	)

	nodeSpecificDataDir := filepath.Join(s.Config.DataDir, s.Config.ThisService.RaftID)
	if err = os.MkdirAll(nodeSpecificDataDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("couldn't create raft storage directory: %w", err)
	}

	if s.Config.InMemory {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapshotStore = raft.NewInmemSnapshotStore()
	} else {
		logStorePath := path.Join(nodeSpecificDataDir, "bolt")
		if _, err := os.Create(logStorePath); err != nil {
			return nil, fmt.Errorf("couldn't create raft logstore file: %w", err)
		}
		if logStore, err = raftboltdb.NewBoltStore(logStorePath); err != nil {
			return nil, fmt.Errorf("couldn't create raft log store: %w", err)
		}

		stableStorePath := path.Join(nodeSpecificDataDir, "stable")
		if _, err := os.Create(stableStorePath); err != nil {
			return nil, fmt.Errorf("couldn't create raft stablestore file: %w", err)
		}
		if stableStore, err = raftboltdb.NewBoltStore(stableStorePath); err != nil {
			return nil, fmt.Errorf("couldn't create raft stable store: %w", err)
		}

		if snapshotStore, err = raft.NewFileSnapshotStore(nodeSpecificDataDir, 10, os.Stderr); err != nil {
			return nil, fmt.Errorf("couldn't create raft snapshot store: %w", err)
		}
	}

	raftNode, err := raft.NewRaft(raftConfig, s.FSM, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("couldn't create raft instance: %w", err)
	}

	return raftNode, nil
}

func (s *Service) Bootstrap() error {
	if s.Raft == nil {
		return errors.New("can't bootstrap a nil raft instance")
	}

	// hasState, err := raftNode.HasExistingState(logStore, stableStore, snapshotStore)
	// if err != nil {
	// 	return nil, fmt.Errorf("couldn't check existing state: %w", err)
	// }

	// if hasState && nodeConfig.Bootstrap {
	// 	return nil, fmt.Errorf("couldn't bootstrap since cluster had existing state")
	// }

	var servers []raft.Server
	for _, i := range s.Config.ClusterInfo {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(i.RaftID),
			Address: raft.ServerAddress(i.GetRaftAddress()),
		})
	}
	clusterConfig := raft.Configuration{
		Servers: servers,
	}

	// TODO:
	// If dir already has state, BootstrapCluster will error, that’s OK on restarts.
	// question: ignore or return
	// typically ignore if it's "configuration already committed".
	if err := s.Raft.BootstrapCluster(clusterConfig).Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}
	return nil
}

func (s *Service) JoinCluster() error {
	me := s.Config.ThisService
	var urls []string
	for _, i := range s.Config.ClusterInfo {
		if i == *me {
			continue
		}
		//TODO: using rafthost + internal http port to dial other nodes works,
		// because rafthost is set to the service name defined in docker-compose, so it will be resolved through its own network
		// but this becomes a problem when we want to redirect an http request: where do we redirect it? we need to know the nodes external host + port
		// alternatively we set up a reverse proxy between the outside world and our cluster, load balancing each request equally between the nodes and:
		//   a) only serve writes through the leader and we need to track leader node changes (for reads we can have both stale reads and up to date reads)
		///  b) distribute request fairly, redirecting writes to the current leader (easy since a Raft instance knows the leader)
		urls = append(urls, "http://"+i.RaftHost+":"+i.InternalHttpPort+"/join")
	}
	err := TryJoin(*me, urls, time.Second*10)
	if err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}
	return nil
}

// StartHTTP starts the http server
func (s *Service) StartHTTP() error {
	s.Logger.Info(fmt.Sprintf("Starting HTTP server on %s", s.Server.Addr()))
	if err := s.Server.Start(); err != nil {
		s.Logger.Error(fmt.Sprintf("Starting HTTP failed %v", err))
		return err
	}
	return nil
}

// Shutdown terminates both the http server with the supplied timeout and the raft node
func (s *Service) Shutdown(timeout time.Duration) {
	if s.Raft != nil {
		s.Logger.Info("Shutting down Raft node")
		if err := s.Raft.Shutdown().Error(); err != nil {
			s.Logger.Info("Failed to shut down Raft node: %v", "error", err)
		}
	}

	s.Logger.Info("Shutting down HTTP server")
	if err := s.Server.Shutdown(timeout); err != nil && err != http.ErrServerClosed {
		s.Logger.Info("Failed to shut down HTTP server: %v", "error", err)
	}
}

func loadRaftConfig(nodeID string, logLevel string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	// cfg.HeartbeatTimeout = 1 * time.Second
	// cfg.ElectionTimeout = 2 * time.Second
	cfg.LogLevel = logLevel
	return cfg
}
