package config

import (
	"flag"
	"fmt"
	"time"
)

type NodeConfig struct {
	RaftAddr string
	HttpAddr string
	NodeID   string
	Inmem    bool

	DataDir     string
	RaftTimeout time.Duration
}

const (
	defaultHTTPAddr string = "127.0.0.1:8000"
	defaultRaftAddr string = "127.0.0.1:12000"
)

var (
	Config NodeConfig
	// defaultConfig = NodeConfig{
	// 	RaftAddr:    defaultRaftAddr,
	// 	HttpAddr:    defaultHTTPAddr,
	// 	NodeID:      "",
	// 	Inmem:       true,
	// 	DataDir:     "data",
	// 	RaftTimeout: 10 * time.Second,
	// }
)

func init() {
	flag.StringVar(&Config.HttpAddr, "httpaddr", defaultHTTPAddr, fmt.Sprintf("address of the http server (default: %s)", defaultHTTPAddr))
	flag.StringVar(&Config.RaftAddr, "raftaddr", defaultRaftAddr, fmt.Sprintf("address of the raft node (default: %s)", defaultRaftAddr))
	flag.StringVar(&Config.NodeID, "id", "", "id of the node (default is the value of --raftaddr)")
	flag.BoolVar(&Config.Inmem, "inmem", true, "use of in-memory storage (false means the use of durable storage)")
	flag.StringVar(&Config.DataDir, "data", "data", "a path where raft stores for logs and snapshots")
	flag.DurationVar(&Config.RaftTimeout, "timeout", 10*time.Second, "raft timeout duration")
}

// func DefaultConfig() *NodeConfig {
// 	return &defaultConfig
// }

func (c *NodeConfig) Validate() {
	if c.NodeID == "" {
		c.NodeID = c.RaftAddr
	}
}
