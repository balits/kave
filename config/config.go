package config

import (
	"flag"
	"fmt"
	"time"
)

type NodeConfig struct {
	RaftAddr    string
	HttpAddr    string
	NodeID      string
	Inmem       bool
	ClusterSize int
	DataDir     string
	RaftTimeout time.Duration
	LogLevel    string
}

const (
	defaultHTTPAddr string = "127.0.0.1:8000"
	defaultRaftAddr string = "127.0.0.1:12000"
)

var defaultConfig NodeConfig
var Config = &defaultConfig

func init() {
	flag.StringVar(&defaultConfig.HttpAddr, "httpaddr", defaultHTTPAddr, "address of the http server")
	flag.StringVar(&defaultConfig.RaftAddr, "raftaddr", defaultRaftAddr, "address of the raft node")
	flag.StringVar(&defaultConfig.NodeID, "id", "", "id of the node (default is the value of --raftaddr)")
	flag.BoolVar(&defaultConfig.Inmem, "inmem", true, "use of in-memory storage (false means the use of durable storage)")
	flag.IntVar(&defaultConfig.ClusterSize, "size", 1, "the number of nodes in the raft cluster (1, 2 or 3)")
	flag.StringVar(&defaultConfig.DataDir, "data", "data", "path to raft logs and snapshots storage")
	flag.DurationVar(&defaultConfig.RaftTimeout, "timeout", 10*time.Second, "raft timeout duration")
	flag.StringVar(&defaultConfig.LogLevel, "loglevel", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")
}

// func DefaultConfig() *NodeConfig {
// 	return &defaultConfig
// }

func (c *NodeConfig) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}

	switch c.ClusterSize {
	case 1, 2, 3:
	default:
		return fmt.Errorf("invalid cluster size: %d", c.ClusterSize)
	}

	switch c.LogLevel {
	case "DEBUG", "INFO", "WARN", "ERROR":
	default:
		return fmt.Errorf("invalid log level: %s", c.LogLevel)
	}

	return nil
}
