package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/logutil"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/node"
)

func main() {
	cfg := config.LoadConfig()
	// change TextLogger to JsonLogger in prod
	logger := logutil.NewLoggerWithKind(cfg.LogLevel, os.Stdout, logutil.TextLoggerKind).
		With("node_id", cfg.Me.NodeID)

	reg := metrics.InitPrometheus()

	node, err := node.New(cfg, logger, reg)
	if err != nil {
		logger.Error("fatal: failed to create node", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := node.Run(ctx); err != nil {
		logger.Error("Node run failed", "error", err)
		os.Exit(1)
	}
}
