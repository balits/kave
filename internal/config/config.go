package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage"
)

type Config struct {
	Me          Peer
	Bootstrap   bool
	StorageOpts storage.StorageOptions
	LogLevel    slog.Level
	Peers       []Peer
}

type ConfigJson struct {
	storage.StorageOptions
	LogLevel configLogLevel `json:"log_level"`
	Peers    string         `json:"peers"`
}

type configLogLevel = string

const (
	levelDebug configLogLevel = "debug"
	levelInfo  configLogLevel = "info"
	levelWarn  configLogLevel = "warn"
	levelError configLogLevel = "error"
)

func (cj *ConfigJson) validate() error {
	if cj.Dir == "" {
		return errors.New("data dir is required")
	}

	switch cj.StorageOptions.Kind {
	case storage.StorageKindBoltdb, storage.StorageKindInMemory:
		// ok
	default:
		return errors.New("unrecognised storage kind")
	}

	switch cj.LogLevel {
	case levelDebug, levelInfo, levelWarn, levelError:
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
	if err := cj.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config json: %v", err)
	}

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

			if err := n.validateNodeConfig(); err != nil {
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

	var kind storage.StorageKind

	switch cj.Kind {
	case storage.StorageKindBoltdb, storage.StorageKindInMemory:
		kind = cj.StorageOptions.Kind
	default:
		return nil, errors.New("unrecognised storage kind")
	}

	logLevel := configLogLevelToSlogLogLevel(cj.LogLevel)

	c := &Config{
		StorageOpts: storage.StorageOptions{
			Kind:           kind,
			Dir:            cj.Dir,
			InitialBuckets: kv.AllBuckets,
		},
		LogLevel: logLevel,
		Peers:    peers,
	}

	return c, nil
}

func configLogLevelToSlogLogLevel(in configLogLevel) (out slog.Level) {
	switch in {
	case levelDebug:
		out = slog.LevelDebug
	case levelInfo:
		out = slog.LevelInfo
	case levelWarn:
		out = slog.LevelWarn
	case levelError:
		out = slog.LevelError
	}
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

func LoadConfig() *Config {
	fs := flag.NewFlagSet("kave", flag.ExitOnError)

	bootstrapMaybe, _ := os.LookupEnv("BOOTSTRAP")
	bootstrapDefault := bootstrapMaybe == "TRUE" || bootstrapMaybe == "true"

	var (
		configPath = fs.String("config", "", "path to config file (required)")
		bootstrap  = fs.Bool("bootstrap", bootstrapDefault, "flag to start bootstrap process")
		nodeID     = fs.String("nodeID", getEnv("NODE_ID"), "id of the raft node")
		raftPort   = fs.String("raft_port", getEnv("RAFT_PORT"), "raft port of the raft node")
		httpPort   = fs.String("http_port", getEnv("HTTP_PORT"), "http port of the raft node")
	)

	fs.Parse(os.Args[1:])

	if *configPath == "" {
		check(errors.New("no config file found"))
	}

	me := Peer{
		NodeID:   *nodeID,
		RaftPort: *raftPort,
		HttpPort: *httpPort,
	}
	check(me.validateNodeConfig())

	var cj ConfigJson
	file, err := os.Open(*configPath)
	check(err)
	d := json.NewDecoder(file)
	d.DisallowUnknownFields()
	check(d.Decode(&cj))

	cfg, err := cj.ToConfig()
	check(err)

	cfg.Me = me
	cfg.Bootstrap = *bootstrap

	return cfg
}

func check(err error) {
	if err != nil {
		panic(fmt.Errorf("fatal error: %v", err))
	}
}
