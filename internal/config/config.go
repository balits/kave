package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/balits/kave/internal/compactor"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
)

// TOODO: move to config
const ApplyLagReadinessThreshold uint = 10

type Flags struct {
	ConfigPath string
	Bootstrap  bool
	NodeID     string
	RaftPort   string
	HttpPort   string
}

type Config struct {
	Me                 Peer
	Bootstrap          bool
	StorageOpts        storage.StorageOptions
	CompactorOpts      compactor.CompactorOptions
	CheckpointInterval time.Duration
	LogLevel           slog.Level
	Peers              []Peer
}

type ConfigJson struct {
	storage.StorageOptions
	compactor.CompactorOptions
	CheckpointInterval time.Duration  `json:"checkpoint_interval_ns"`
	LogLevel           configLogLevel `json:"log_level"`
	Peers              string         `json:"peers"`
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

	return cj.CompactorOptions.Validate()
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

	logLevel := configLogLevelToSlogLogLevel(cj.LogLevel)

	c := &Config{
		LogLevel:           logLevel,
		Peers:              peers,
		CheckpointInterval: cj.CheckpointInterval,
		CompactorOpts:      cj.CompactorOptions,
		StorageOpts: storage.StorageOptions{
			Kind:           cj.StorageOptions.Kind,
			Dir:            cj.Dir,
			InitialBuckets: schema.AllBuckets,
		},
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

func RegisterFlags() Flags {
	var f Flags
	fs := flag.NewFlagSet("kave", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: kave --config=PATH --node_id=ID --raft_port=RAFT_PORT --http_port=HTTP_PORT")
		fs.PrintDefaults()
	}

	bootstrapMaybe, _ := os.LookupEnv("BOOTSTRAP")
	bootstrapDefault := bootstrapMaybe == "TRUE" || bootstrapMaybe == "true"

	fs.StringVar(&f.ConfigPath, "config", "", "path to config file (required)")
	fs.BoolVar(&f.Bootstrap, "bootstrap", bootstrapDefault, "flag to start bootstrap process")
	fs.StringVar(&f.NodeID, "nodeID", getEnv("NODE_ID"), "id of the raft node")
	fs.StringVar(&f.RaftPort, "raft_port", getEnv("RAFT_PORT"), "raft port of the raft node")
	fs.StringVar(&f.HttpPort, "http_port", getEnv("HTTP_PORT"), "http port of the raft node")

	fs.Parse(os.Args[1:])

	if f.ConfigPath == "" {
		check(errors.New("no config path given"))
	}
	if f.NodeID == "" {
		check(errors.New("no node_id given"))
	}
	if f.RaftPort == "" {
		check(errors.New("no raft_port given"))
	}
	if f.HttpPort == "" {
		check(errors.New("no http_port given"))
	}
	return f
}

func LoadConfig(f Flags) *Config {
	me := Peer{
		NodeID:   f.NodeID,
		RaftPort: f.RaftPort,
		HttpPort: f.HttpPort,
	}
	check(me.validateNodeConfig())

	var cj ConfigJson
	file, err := os.Open(f.ConfigPath)
	check(err)
	d := json.NewDecoder(file)
	d.DisallowUnknownFields()
	check(d.Decode(&cj))

	cfg, err := cj.ToConfig()
	check(err)

	cfg.Me = me
	cfg.Bootstrap = f.Bootstrap

	return cfg
}

func check(err error) {
	if err != nil {
		panic(fmt.Errorf("fatal error: %v", err))
	}
}
