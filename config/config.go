package config

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
)

// Config TODO: docs and switch config everywhere from v1 to v2
// that will help us use the various GetXAddress methods for redirection, joining, web server creation etc
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
	RaftHost         string `json:"raft_host"`          // host part of the address of the raft node, for inter-node communication (raft operations)
	RaftPort         string `json:"raft_port"`          // port part of the address of the raft node, for inter-node communication (raft operations)
	HttpHost         string `json:"http_host"`          // host part of the external http server exposed by the node
	InternalHttpPort string `json:"internal_http_port"` // port part of the internal http server, for intern-node communication (joining cluster)
	ExternalHttpPort string `json:"external_http_port"` // port part of the external http server, for the exposed http server (store endpoints like /get /set)
	NeedBootstrap    bool   `json:"need_bootstrap"`     // flags that that this service should bootstrap the raft cluster
}

// GetInternalHttpAddress returns the internal http address of the service
// func (s ServiceInfo) GetInternalHttpAddress() string {
// 	return s.RaftHost + ":" + s.InternalHttpPort
// }

// // GetExternalHttpAddress returns the external http address of the service
// func (s ServiceInfo) GetExternalHttpAddress() string {
// 	return s.RaftHost + ":" + s.ExternalHttpPort
// }

// GetRaftAddress returns the address of the raft node, used by hc's raft librar
func (s ServiceInfo) GetRaftAddress() string {
	return s.RaftHost + ":" + s.RaftPort
}

func (s ServiceInfo) Validate() error {
	if s.RaftID == "" {
		return errors.New("raft id is required")
	}
	if s.RaftHost == "" {
		return errors.New("raft host is required")
	}
	if s.RaftPort == "" {
		return errors.New("raft port is required")
	}
	if s.HttpHost == "" {
		return errors.New("http host is required")
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
		return nil, errors.New("no config file was provided")
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
		return nil, errors.New("invalid log level \"" + cfg.LogLevel + "\"")
	}

	switch len(cfg.ClusterInfo) {
	case 1, 3, 5:
	default:
		return nil, errors.New("raft clusters size must be 1, 3 or 5")
	}

	var hasBootstrapper bool
	for _, info := range cfg.ClusterInfo {
		if err = info.Validate(); err != nil {
			return nil, err
		}
		if info.NeedBootstrap {
			if hasBootstrapper {
				return nil, errors.New("duplicate bootstrapper node found in cluster info")
			}
			hasBootstrapper = true
		}
		if info.RaftID == nodeIDFlag {
			if cfg.ThisService != nil {
				return nil, errors.New("duplicate node ids found in cluster info")
			}
			cfg.ThisService = &info
		}
	}

	if cfg.ThisService == nil {
		return nil, errors.New("node with with the supplied nodeid not found in cluster info")
	}

	if !hasBootstrapper {
		return nil, errors.New("no node was given the role of bootstrapping the cluster")
	}

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
