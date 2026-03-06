package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/node"
	"github.com/balits/kave/internal/util"
)

func main() {
	cfg := config.LoadConfig()
	// change TextLogger to JsonLogger in prod
	logger := util.NewLoggerWithKind(cfg.LogLevel, os.Stdout, util.TextLoggerKind).
		With("node_id", cfg.Me.NodeID)

	node, err := node.New(cfg, logger)
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
