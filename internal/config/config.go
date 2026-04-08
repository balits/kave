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

	"github.com/balits/kave/internal/compaction"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
)

const ApplyLagReadinessThreshold uint = 10

type Config struct {
	Bootstrap                 bool
	Me                        peer.Peer
	KvOptions                 kv.Options
	PeerDiscoveryOptions      peer.DiscoveryOptions
	StorageOpts               storage.StorageOptions
	CompactionOpts            compaction.CompactionOptions
	OtOpts                    ot.Options
	CheckpointIntervalMinutes time.Duration
	LogLevel                  slog.Level
}

type ConfigJson struct {
	KvOptions                 kv.Options                   `json:"kv"`
	DiscoveryOptions          peer.DiscoveryOptions        `json:"peer_discovery"`
	StorageOpts               storage.StorageOptions       `json:"storage"`
	CompactionOpts            compaction.CompactionOptions `json:"compaction"`
	OtOpts                    ot.Options                   `json:"ot"`
	CheckpointIntervalMinutes time.Duration                `json:"checkpoint_interval_minutes"`
	LogLevel                  configLogLevel               `json:"log_level"`
}

type configLogLevel = string

const (
	levelDebug configLogLevel = "debug"
	levelInfo  configLogLevel = "info"
	levelWarn  configLogLevel = "warn"
	levelError configLogLevel = "error"
)

func (cj *ConfigJson) check() error {
	if err := cj.StorageOpts.Check(); err != nil {
		return err
	}
	if err := cj.CompactionOpts.Check(); err != nil {
		return err
	}
	if err := cj.OtOpts.Check(); err != nil {
		return err
	}
	if err := cj.DiscoveryOptions.Check(); err != nil {
		return err
	}
	if err := cj.KvOptions.Check(); err != nil {
		return err
	}

	return nil
}

// ToConfig parses the raw json configuration, and turns them
// into a usable Config, or returns with an error.
// It also removes a given node from the peer list
func (cj *ConfigJson) ToConfig() (*Config, error) {
	if err := cj.check(); err != nil {
		return nil, fmt.Errorf("failed to validate config json: %v", err)
	}

	logLevel := configLogLevelToSlogLogLevel(cj.LogLevel)

	c := &Config{
		LogLevel:                  logLevel,
		CheckpointIntervalMinutes: cj.CheckpointIntervalMinutes,
		CompactionOpts:            cj.CompactionOpts,
		OtOpts:                    cj.OtOpts,
		StorageOpts: storage.StorageOptions{
			Kind:           cj.StorageOpts.Kind,
			Dir:            cj.StorageOpts.Dir,
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

func getEnvOrPanic(key string) string {
	v, ok := os.LookupEnv(key)
	v = strings.TrimSpace(v)
	if !ok || v == "" {
		panic(fmt.Sprintf("env var %s not set", key))
	}
	return v
}

func LoadConfig() *Config {
	fs := flag.NewFlagSet("kave", flag.ExitOnError)

	var (
		configPath = fs.String("config", "", "path to config file (required)")
		nodeID     = fs.String("nodeID", getEnvOrPanic("POD_NAME"), "id of the raft node")
		raftPort   = fs.String("raft_port", getEnvOrPanic("RAFT_PORT"), "raft port of the raft node")
		httpPort   = fs.String("http_port", getEnvOrPanic("HTTP_PORT"), "http port of the raft node")
		// role       = fs.String("role", getEnv("NDOE_ROLE"), "role of the node (voter | learner)")
	)

	err := fs.Parse(os.Args[1:])
	check("parsing flagset", err)

	if *configPath == "" {
		check("config file", errors.New("no config file found"))
	}

	me := peer.Peer{
		NodeID:   *nodeID,
		RaftPort: *raftPort,
		HttpPort: *httpPort,
	}
	check("peer info about me", me.Check())

	var cj ConfigJson
	file, err := os.Open(*configPath)
	check("config file", err)

	d := json.NewDecoder(file)
	d.DisallowUnknownFields()
	check("config file: decoding: ", d.Decode(&cj))

	cfg, err := cj.ToConfig()
	check("config file: converting json to config object", err)

	cfg.Bootstrap = strings.HasSuffix(*nodeID, "-0")
	cfg.Me = me

	return cfg
}

func check(msg string, err error) {
	if err != nil {
		panic(fmt.Errorf("fatal error: %s: %v", msg, err))
	}
}
