package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/node"
	"github.com/balits/kave/internal/util"
)

func main() {
	cfg, err := config.LoadConfig()
	fatal(err, nil)

	logger := util.NewLoggerWithKind(cfg.LogLevel, os.Stdout, util.TextLoggerKind).
		With("node_id", cfg.Me.NodeID)

	node, err := node.New(cfg, logger)
	fatal(err, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := node.Run(ctx); err != nil {
		logger.Error("Node run failed", "error", err)
		os.Exit(1)
	}
}

func fatal(err error, logger *slog.Logger) {
	if err != nil {
		if logger != nil {
			logger.Error("Fatal error occured", "error", err)
		} else {
			fmt.Println("Fatal error occured: ", err)
		}
		os.Exit(1)
	}
}
