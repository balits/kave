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
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
)

const ApplyLagReadinessThreshold uint = 10

type Config struct {
	Me                        Peer
	Bootstrap                 bool
	StorageOpts               storage.StorageOptions
	CompactionOpts            compaction.CompactionOptions
	OtOpts                    ot.Options
	CheckpointIntervalMinutes time.Duration
	LogLevel                  slog.Level
	Peers                     []Peer
}

type ConfigJson struct {
	StorageOpts               storage.StorageOptions       `json:"storage"`
	CompactionOpts            compaction.CompactionOptions `json:"compaction"`
	OtOpts                    ot.Options                   `json:"ot"`
	CheckpointIntervalMinutes time.Duration                `json:"checkpoint_interval_minutes"`
	LogLevel                  configLogLevel               `json:"log_level"`
	Peers                     string                       `json:"peers"`
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

	return nil
}

// ToConfig parses the raw json configuration, and turns them
// into a usable Config, or returns with an error.
// It also removes a given node from the peer list
func (cj *ConfigJson) ToConfig() (*Config, error) {
	if err := cj.check(); err != nil {
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

			if err := n.check(); err != nil {
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
		LogLevel:                  logLevel,
		Peers:                     peers,
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

	var (
		configPath = fs.String("config", "", "path to config file (required)")
		nodeID     = fs.String("nodeID", getEnv("POD_NAME"), "id of the raft node")
		raftPort   = fs.String("raft_port", getEnv("RAFT_PORT"), "raft port of the raft node")
		httpPort   = fs.String("http_port", getEnv("HTTP_PORT"), "http port of the raft node")
		// role       = fs.String("role", getEnv("NDOE_ROLE"), "role of the node (voter | learner)")
	)

	err := fs.Parse(os.Args[1:])
	check(err)

	if *configPath == "" {
		check(errors.New("no config file found"))
	}

	me := Peer{
		NodeID:   *nodeID,
		RaftPort: *raftPort,
		HttpPort: *httpPort,
	}
	check(me.check())

	var cj ConfigJson
	file, err := os.Open(*configPath)
	check(err)
	d := json.NewDecoder(file)
	d.DisallowUnknownFields()
	check(d.Decode(&cj))

	cfg, err := cj.ToConfig()
	check(err)

	cfg.Me = me
	cfg.Bootstrap = strings.HasSuffix(*nodeID, "-0")

	return cfg
}

func check(err error) {
	if err != nil {
		panic(fmt.Errorf("fatal error: %v", err))
	}
}
