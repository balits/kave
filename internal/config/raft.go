package config

// func loadRaftConfig(nodeID string, logLevel string, logger *slog.Logger) *raft.Config {
// 	cfg := raft.DefaultConfig()
// 	cfg.LocalID = raft.ServerID(nodeID)
// 	cfg.LogLevel = logLevel
// 	cfg.Logger = util.NewHcLogAdapter(logger.With("module", "raftlib"), util.StringToSlogLevel(logLevel))
// 	return cfg
// }
