package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type StorageKind string

const (
	StorageKindInMemory StorageKind = "inmemory"
	StorageKindBolt     StorageKind = "boltdb"
)

// Config is the global configuration pertaining to this node. It contains
// information about the cluster (peers, log level) and about this node (ID, ports, data directory)
//
// TODO: add enableLogging flag if we ever want to run in quiet mode: both hc's raft and our logger should be nil or DiscardLogger
type Config struct {
	// Node specific info
	Peer           // embedded Peer information about us
	Bootstrap bool // should we

	// Cluster wide configs
	Storage  StorageKind
	LogLevel slog.Level
	Dir      string // data directory to save runtime data + persistence // TODO: we can let nodes handles where they wanna store data, this isnt rly a cluster wide info

	// all the nodes in the cluster including us
	Peers []Peer
}

// FindPeer returns the peer with the specified nodeID
func (c *Config) FindPeer(nodeID string) (info *Peer, ok bool) {
	for _, i := range c.Peers {
		if i.NodeID == nodeID {
			info = &i
			ok = true
			break
		}
	}
	return
}

type ConfigLogLevel = string

const (
	LogLevelDebug ConfigLogLevel = "debug"
	LogLevelInfo  ConfigLogLevel = "info"
	LogLevelWarn  ConfigLogLevel = "warn"
	LogLevelError ConfigLogLevel = "error"
)

func ConfigLogLevelToSlogLogLevel(l ConfigLogLevel) (out slog.Level) {
	switch l {
	case LogLevelDebug:
		out = slog.LevelDebug
	case LogLevelInfo:
		out = slog.LevelInfo
	case LogLevelWarn:
		out = slog.LevelWarn
	case LogLevelError:
		out = slog.LevelError
	}
	return
}

// ConfigJson handles reading a structured config from json files, and turning them into proper Configs
type ConfigJson struct {
	Storage  StorageKind    `json:"storage"`
	LogLevel ConfigLogLevel `json:"log_level"`
	Dir      string         `json:"dir"`
	Peers    string         `json:"peers"`
}

func (cj *ConfigJson) Validate() error {
	if cj.Dir == "" {
		return errors.New("data dir is required")
	}

	switch cj.Storage {
	case StorageKindBolt, StorageKindInMemory:
		// ok
	default:
		return errors.New("unrecognised storage kind")
	}

	switch cj.LogLevel {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
		// ok
	default:
		return errors.New("unrecognised loglevel kind")
	}

	return nil
}

// ToConfig parses the raw json configuration, and turns them
// into a usable Config, or returns with an error.
// It also removes a given node from the peer list
func (cj *ConfigJson) ToConfig() (*Config, error) {
	var peers []Peer

	if strings.TrimSpace(cj.Peers) == "" {
		peers = make([]Peer, 0)
	} else {
		rawPeers := strings.SplitSeq(cj.Peers, ",")
		for s := range rawPeers {
			parts := strings.SplitN(s, ":", 3)
			if len(parts) != 3 {
				return nil, fmt.Errorf("failed to parse peer %s (expected id:raft_port:http_port)", s)
			}

			n := Peer{
				NodeID:   parts[0],
				RaftPort: parts[1],
				HttpPort: parts[2],
			}

			if err := n.ValidateNodeConfig(); err != nil {
				return nil, err
			}
			peers = append(peers, n)
		}
	}

	switch len(peers) {
	case 1, 3, 5:
	default:
		return nil, fmt.Errorf("for optimal raft clusters, cluster size must be 1, 3 or 5, got %d", len(peers))
	}

	var storage StorageKind

	switch cj.Storage {
	case StorageKindBolt, StorageKindInMemory:
		storage = cj.Storage
	default:
		return nil, errors.New("unrecognised storage kind")
	}

	logLevel := ConfigLogLevelToSlogLogLevel(cj.LogLevel)

	c := &Config{
		Storage:  storage,
		LogLevel: logLevel,
		Dir:      cj.Dir,
		Peers:    peers,
	}

	return c, nil
}
func LoadConfig() (*Config, error) {
	// set flags in a function rather than init()
	// so that our tests dont need the same flags to be set
	fs := flag.NewFlagSet("thesis", flag.ExitOnError)

	bootstrapRaw := os.Getenv("BOOTSTRAP")
	var bootstrapDefault bool
	switch bootstrapRaw {
	case "1", "TRUE", "true":
		bootstrapDefault = true
	}

	configPath := fs.String("config", "", "path to the cluster configuration json file")
	bootstrap := fs.Bool("bootstrap", bootstrapDefault, "flag to indicate if node should bootstrap the cluster")
	nodeId := fs.String("node_id", getEnv("NODE_ID"), "id of the raft node")
	raftPort := flag.String("raft_port", getEnv("RAFT_PORT"), "port of the raft node")
	httpPort := flag.String("http_port", getEnv("HTTP_PORT"), "port of the node's http server")

	fs.Parse(os.Args[1:])

	if *configPath == "" {
		return nil, errors.New("no config file was provided")
	}

	p := Peer{
		NodeID:   *nodeId,
		RaftPort: *raftPort,
		HttpPort: *httpPort,
	}

	// validate node config flags
	if err := p.ValidateNodeConfig(); err != nil {
		return nil, err
	}
	fmt.Printf("Peer %+v\n", p)

	// parse json from config flag path
	configJson, err := newConfigJson(*configPath)
	if err != nil {
		return nil, err
	}
	fmt.Printf("ConfigJSON %+v\n", configJson)

	// convert to valid config
	config, err := configJson.ToConfig()
	if err != nil {
		return nil, err
	}
	fmt.Printf("Config %+v\n", config)

	config.Bootstrap = *bootstrap
	config.Peer = p
	return config, nil
}

func newConfigJson(path string) (cj ConfigJson, err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	err = dec.Decode(&cj)
	return
}

// NOTE: may panic if envvar is not set
func getEnv(key string) string {
	v, ok := os.LookupEnv(key)
	v = strings.TrimSpace(v)
	if !ok || v == "" {
		panic(fmt.Sprintf("env var %s not set", key))
	}
	return v
}
