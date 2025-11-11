package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/util"
)

func main() {
	doneCh := make(chan os.Signal, 1)
	errorCh := make(chan error)
	signal.Notify(doneCh, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := config.LoadConfig()
	fatal(err, nil)

	logger := util.NewLoggerWithKind(config.LogLevel, os.Stdout, util.TextLoggerKind)
	logger.Debug(fmt.Sprintf("config loaded: %+v", config))

	nodeEnv, err := raftnode.NewEnv(config, logger)
	fatal(err, logger)

	node := raftnode.CreateNode(nodeEnv)
	fatal(err, node.Logger)

	go func() {
		httpAddr := config.GetInternalHttpAddress()
		server := api.NewServer(httpAddr, node, logger)
		errorCh <- server.Run(ctx)
	}()

	go func() {
		errorCh <- node.Run(ctx)
	}()

	select {
	case sig := <-doneCh:
		logger.Error("Process terminated", "signal", sig)
	case err := <-errorCh:
		logger.Error("An error occured, terminating process", "error", err)
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
