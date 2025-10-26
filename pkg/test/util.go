package test

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/balits/thesis/pkg/config"
	"github.com/balits/thesis/pkg/service"
	"github.com/balits/thesis/pkg/store"
	"github.com/balits/thesis/pkg/web"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

func AssertEventually(t *testing.T, condition func() bool, timeout, interval time.Duration) {
	var (
		tryConditionCh = make(chan bool)
		checkCondition = func() { tryConditionCh <- condition() }
		timer          = time.NewTimer(timeout)
		ticker         = time.NewTicker(interval)
	)
	defer func() {
		defer timer.Stop()
		defer ticker.Stop()
	}()
	go checkCondition() // initial check so we dont wait for first tick

	// different ticker channel bcs  fist call to checkCondition takes time, during which ticker could tick
	// instead set it to nil before check, and back to ticker.C after failed assertion
	var tickerCh <-chan time.Time
	for {
		select {
		case <-timer.C:
			t.Errorf("Some nodes failed to join the cluster")
		case <-tickerCh:
			tickerCh = nil
			go checkCondition()
		case result := <-tryConditionCh:
			if result {
				t.Log("All nodes joined the cluster")
				return
			} else {
				tickerCh = ticker.C
			}
		}
	}
}

func NewMockService(info config.ServiceInfo, mocklogger *slog.Logger, config *config.Config) *service.Service {
	httpAddr := fmt.Sprintf("%s:%s", info.HttpHost, info.ExternalHttpPort)
	mockserver := web.NewServer(httpAddr, web.NewRouter())
	s := service.NewService(MockStore, MockFSM, mockserver, mocklogger, config)
	return s
}

func NewMockInmemRaft(svc *service.Service, trans *raft.InmemTransport) (*raft.Raft, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(svc.Config.ThisService.RaftID)
	raftCfg.LogLevel = "INFO"
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	return raft.NewRaft(raftCfg, svc.FSM, logs, stable, snaps, trans)
}

func NewMockDurableRaft(svc *service.Service, tempdir string) (*raft.Raft, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(svc.Config.ThisService.RaftID)
	raftCfg.LogLevel = "INFO"
	logs, err := raftboltdb.NewBoltStore(path.Join(tempdir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(tempdir, 1, os.Stdout)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(svc.Config.ThisService.GetRaftAddress(), nil, 3, 2*time.Second, os.Stdout)
	if err != nil {
		return nil, err
	}
	return raft.NewRaft(raftCfg, svc.FSM, logs, logs, snaps, transport)
}

func NewMockConfig(nNodes int) *config.Config {
	clusterInfo := make([]config.ServiceInfo, nNodes)
	for i := range nNodes {
		clusterInfo[i] = config.ServiceInfo{
			RaftID:           fmt.Sprintf("node%d", i),
			RaftHost:         "127.0.0.1",
			RaftPort:         fmt.Sprintf("700%d", i),
			HttpHost:         "127.0.0.1",
			InternalHttpPort: fmt.Sprintf("800%d", i),
			ExternalHttpPort: fmt.Sprintf("808%d", i),
			NeedBootstrap:    i == 0,
		}
	}

	return &config.Config{
		InMemory:    true,
		LogLevel:    "DEBUG",
		DataDir:     "data",
		ClusterInfo: clusterInfo,
		ThisService: nil,
	}
}

func NewConfigForNode(cfg *config.Config) *config.Config {
	return &config.Config{
		InMemory:    cfg.InMemory,
		LogLevel:    cfg.LogLevel,
		DataDir:     cfg.DataDir,
		ClusterInfo: cfg.ClusterInfo,
		ThisService: nil,
	}
}

var (
	MockStore = store.NewInMemoryStore()
	MockFSM   = service.NewFSM(MockStore)
)

// type mockstore struct{}

// func (s *mockStore) Set(key string, value []byte) error            { return nil }
// func (s *mockStore) Delete(key string) (value []byte, err error)   { return nil, nil }
// func (s *mockStore) GetStale(key string) (value []byte, err error) { return nil, nil }
// func (s *mockStore) Snapshot() (raft.FSMSnapshot, error)           { return nil, nil }
// func (s *mockStore) Restore(snapshot io.ReadCloser) error          { return nil }

// type nullWriter struct{}

// func (w nullWriter) Write([]byte) (int, error) { return 0, nil }

func NewMockLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

func NewInmemCluster(baseCfg *config.Config) ([]*service.Service, error) {
	// var (
	// 	wg                    sync.WaitGroup
	// 	transports            = make([]*raft.InmemTransport, len(baseCfg.ClusterInfo))
	// 	transportsConnectedCh = make(chan struct{})
	// 	errorCh               = make(chan error)
	// 	services              = make([]*service.Service, len(baseCfg.ClusterInfo))
	// )

	// transports = make([]*raft.InmemTransport, len(baseCfg.ClusterInfo))
	// for i, info := range baseCfg.ClusterInfo {
	// 	_, transports[i] = raft.NewInmemTransport(raft.ServerAddress(info.RaftID))
	// }

	// for i, info := range baseCfg.ClusterInfo {
	// 	svcCfg := NewConfigForNode(baseCfg)
	// 	svcCfg.ThisService = &info
	// 	services[i] = NewMockService(info, NewMockLogger(), svcCfg)
	// 	services[i].RegisterRoutes()
	// 	services[i].StartHTTP()
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		var r *raft.Raft
	// 		r, err := NewMockInmemRaft(services[i], transports[i])
	// 		if err != nil {
	// 			errorCh <- fmt.Errorf("error creating raft instance: %w", err)
	// 			return
	// 		}
	// 		if r == nil {
	// 			errorCh <- fmt.Errorf("unexpected error: raft insance was nil")
	// 			return
	// 		}
	// 		services[i].Raft = r
	// 		<-transportsConnectedCh

	// 		needBootstrap := services[i].Config.ThisService.NeedBootstrap
	// 		if needBootstrap {
	// 			servers := make([]raft.Server, len(baseCfg.ClusterInfo))
	// 			for i, info2 := range services[i].Config.ClusterInfo {
	// 				servers[i] = raft.Server{
	// 					ID:      raft.ServerID(info2.RaftID),
	// 					Address: transports[i].LocalAddr(),
	// 				}
	// 			}

	// 			if err = services[i].Raft.BootstrapCluster(raft.Configuration{
	// 				Servers: servers,
	// 			}).Error(); err != nil {
	// 				errorCh <- fmt.Errorf("error bootstrapping the cluster: %w", err)
	// 				return
	// 			}
	// 		}
	// 	}()
	// }

	// for i := range len(transports) {
	// 	for j := range len(transports) {
	// 		if i == j {
	// 			continue
	// 		}
	// 		transports[i].Connect(transports[j].LocalAddr(), transports[j])
	// 	}
	// }
	// close(transportsConnectedCh) // broadcast that transports are hooked up
	// wg.Wait()                    // every node is joining or had boostrapped
	// select {
	// case err := <-errorCh:
	// 	return nil, err
	// case <-services[0].Raft.LeaderCh():
	// 	// first leader elected
	// 	return services, nil
	// case <-time.After(time.Duration(5 * time.Second)):
	// 	return nil, errors.New("cluster creation timed out: no errors got from nodes, but no leader was electer either")
	// }
	return nil, nil
}

func NewDurableCluster(t *testing.T, baseCfg *config.Config, logger *slog.Logger) ([]*service.Service, error) {
	var (
		wg       sync.WaitGroup
		errorCh  = make(chan error, len(baseCfg.ClusterInfo))
		services = make([]*service.Service, len(baseCfg.ClusterInfo))
	)

	for i, info := range baseCfg.ClusterInfo {
		nodeIndex := i
		nodeInfo := info
		wg.Add(1)
		fmt.Println("starting go routine for ", nodeInfo.RaftID)
		go func(nodeInfo config.ServiceInfo, nodeIndex int, tempdir string) {
			defer func() {
				fmt.Println("stopped go routine for", nodeInfo.RaftID)
				wg.Done()
			}()
			svcCfg := NewConfigForNode(baseCfg)
			svcCfg.ThisService = &nodeInfo
			services[nodeIndex] = NewMockService(nodeInfo, logger, svcCfg)
			services[nodeIndex].RegisterRoutes()
			if err := os.MkdirAll(tempdir, 0o755); err != nil {
				errorCh <- fmt.Errorf("error creating tempdir: %w", err)
				return
			}
			r, err := NewMockDurableRaft(services[nodeIndex], tempdir)
			if err != nil {
				errorCh <- fmt.Errorf("error creating raft instance: %w", err)
				return
			}
			if r == nil {
				errorCh <- fmt.Errorf("unexpected error: raft insance was nil")
				return
			}
			services[nodeIndex].Raft = r
			services[nodeIndex].Logger.Info("Service created", "node", services[nodeIndex].Config.ThisService.RaftID, "service", fmt.Sprintf("%+v", services[nodeIndex]))
			go func(svc *service.Service) {
				if err = svc.StartHTTP(); err != nil {
					errorCh <- err
				}
			}(services[nodeIndex])
			if services[nodeIndex].Config.ThisService.NeedBootstrap {
				servers := make([]raft.Server, len(baseCfg.ClusterInfo))
				for i, info2 := range services[nodeIndex].Config.ClusterInfo {
					servers[i] = raft.Server{
						ID:      raft.ServerID(info2.RaftID),
						Address: raft.ServerAddress(info2.GetRaftAddress()),
					}
				}

				services[nodeIndex].Logger.Info("Bootstrapping", "node", services[nodeIndex].Config.ThisService.RaftID)
				if err = services[nodeIndex].Raft.BootstrapCluster(raft.Configuration{
					Servers: servers,
				}).Error(); err != nil {
					errorCh <- fmt.Errorf("error bootstrapping the cluster: %w", err)
					return
				}
			} else {
				me := services[nodeIndex].Config.ThisService
				var urls []string
				for _, i := range services[nodeIndex].Config.ClusterInfo {
					if i == *me {
						continue
					}
					urls = append(urls, "http://"+i.RaftHost+":"+i.ExternalHttpPort+"/join")
				}
				services[nodeIndex].Logger.Info("Trying to joind", "node", services[nodeIndex].Config.ThisService.RaftID, "target_urls", fmt.Sprintf("%v", urls))
				err = service.TryJoin(*me, urls, time.Duration(5*time.Millisecond))
				if err != nil {
					errorCh <- fmt.Errorf("failed to join cluster %w", err)
					return
				}
			}
		}(nodeInfo, nodeIndex, filepath.Join(t.TempDir(), nodeInfo.RaftID))
	}

	wg.Wait() // every node is joining or had boostrapped
	if services[0].Raft == nil {
		select {
		case err := <-errorCh:
			return nil, fmt.Errorf("leaders raft instance was nil: %v", err)
		default:
			return nil, fmt.Errorf("leaders raft instance was nil")
		}

	}
	select {
	case <-services[0].Raft.LeaderCh(): // first leader elected
		return services, nil
	case err := <-errorCh:
		return nil, err // return on first error
	case <-time.After(time.Duration(25 * time.Second)):
		return nil, errors.New("cluster creation timed out: no errors got from nodes, but no leader was electer either")
	}
}
