package config

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
)

// TODO: add enableLogging flag if we ever want to run in quiet mode: both hc's raft and our logger should be nil or DiscardLogger
type Config struct {
	InMemory    bool          `json:"inmem"`     // true for ephemeral storage, false for persistent storage
	LogLevel    string        `json:"log_level"` // DEBUG | INFO | WARN | ERROR
	DataDir     string        `json:"data_dir"`  // data to save runtime data + persistence
	ClusterInfo []ServiceInfo `json:"cluster_info"`

	// ThisService is the ServiceInfo associated with this node
	ThisService *ServiceInfo
}

func (c Config) Validate() error {
	if c.DataDir == "" && !c.InMemory {
		return errors.New("data dir is required when not running in memory")
	}
	if c.DataDir != "" && c.InMemory {
		return errors.New("data dir should not be set when running in memory")
	}
	if c.LogLevel == "" {
		return errors.New("log level is required")
	}
	if len(c.ClusterInfo) == 0 {
		return errors.New("cluster info is required")
	}
	return nil

}

// Service describes an instance of a running service, including its raftID and its various addresses
type ServiceInfo struct {
	RaftID           string `json:"id"`                 // ID of the raft node
	InternalHost     string `json:"internal_host"`      // internal host  of the node, for inter-node communication (raft operations)
	RaftPort         string `json:"raft_port"`          // (internal) port of the address of the node, for inter-node communication (raft operations)
	ExternalHost     string `json:"external_host"`      // external hos  of the node, exposed for its public http server
	InternalHttpPort string `json:"internal_http_port"` // internal part of the node for its public http server
	ExternalHttpPort string `json:"external_http_port"` // external (advertised) port of the node for its public http server
	NeedBootstrap    bool   `json:"need_bootstrap"`     // flags that that this service should bootstrap the raft cluster
}

// GetRaftAddress returns the (internal) address of the raft node, used by hc's raft librar
func (s ServiceInfo) GetRaftAddress() string {
	return s.InternalHost + ":" + s.RaftPort
}

// GetInternalHttpAddress returns the (internal) address of the nodes public http server
func (s ServiceInfo) GetInternalHttpAddress() string {
	return s.InternalHost + ":" + s.InternalHttpPort
}

// GetExternalHttpAddress returns the (external, advertised) address of the nodes public http server
func (s ServiceInfo) GetExternalHttpAddress() string {
	return s.ExternalHost + ":" + s.ExternalHttpPort
}

func (s ServiceInfo) Validate() error {
	if s.RaftID == "" {
		return errors.New("raft id is required")
	}
	if s.InternalHost == "" {
		return errors.New("internal host is required")
	}
	if s.RaftPort == "" {
		return errors.New("raft port is required")
	}
	if s.ExternalHost == "" {
		return errors.New("external host is required")
	}
	if s.InternalHttpPort == "" {
		return errors.New("internal http port is required")
	}
	if s.ExternalHttpPort == "" {
		return errors.New("external http port is required")
	}
	return nil
}

var (
	configFlag string
	nodeIDFlag string
)

func init() {
	flag.StringVar(&configFlag, "config", "", "path to the cluster configuration json file")
	flag.StringVar(&nodeIDFlag, "nodeid", "", "id of the raft node")
}

func LoadConfig() (*Config, error) {
	flag.Parse()

	if configFlag == "" {
		return nil, errors.New("config error: no config file was provided")
	}
	cfg, err := parseConfigFile(configFlag)
	if err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	switch cfg.LogLevel {
	case "DEBUG", "INFO", "WARN", "ERROR":
	default:
		return nil, errors.New("config error: invalid log level \"" + cfg.LogLevel + "\"")
	}

	switch len(cfg.ClusterInfo) {
	case 1, 3, 5:
	default:
		return nil, errors.New("config error: raft clusters size must be 1, 3 or 5")
	}

	var hasBootstrapper bool
	for _, info := range cfg.ClusterInfo {
		if err = info.Validate(); err != nil {
			return nil, err
		}
		if info.NeedBootstrap {
			if hasBootstrapper {
				return nil, errors.New("config error: duplicate bootstrapper node found in cluster info")
			}
			hasBootstrapper = true
		}
		if info.RaftID == nodeIDFlag {
			if cfg.ThisService != nil {
				return nil, errors.New("config error: duplicate node ids found in cluster info")
			}
			cfg.ThisService = &info
		}
	}

	if cfg.ThisService == nil {
		return nil, errors.New("config error: node with with the supplied nodeid not found in cluster info")
	}

	if !hasBootstrapper {
		return nil, errors.New("config error: no node was given the role of bootstrapping the cluster")
	}

	// TODO: REMOVE DATA DIR OPTION ENTIRELY OR USE ENV VARS / FLAGS FOR IT
	// BUT IN COMPOSE WE SET EACH VOLUME TO /DATA SO IDKKK
	cfg.DataDir = "data"

	return &cfg, nil
}

func parseConfigFile(path string) (cfg Config, err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	err = dec.Decode(&cfg)
	return
}
