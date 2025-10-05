package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/balits/thesis/config"
	"github.com/balits/thesis/raftnode"
)

func main() {
	flag.Parse()
	config.Config.Validate()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	node, _ := raftnode.NewNode(config.Config.NodeID, logger)

	go node.StartServer()
	logger.Info("HTTP Server started", "address", config.Config.HttpAddr)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt)
	<-done

	if err := node.ShutdownServer(time.Second * 5); err != nil {
		logger.Error("HTTP Server shutdown failed", "error", err)
	}
}
