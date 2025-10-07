package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/balits/thesis/config"
	"github.com/balits/thesis/node"
	"github.com/balits/thesis/web"
)

func main() {
	flag.Parse()
	err := config.Config.Validate()
	if err != nil {
		fmt.Printf("Invalid config: %v\n", err)
		os.Exit(1)
	}

	logger := newLogger()
	router := web.NewRouter()

	server := web.NewServer(config.Config.HttpAddr, router)
	node, err := node.NewNode(config.Config, logger, server)

	if err != nil {
		logger.Error("Failed to create node", "error", err)
	}

	node.RegisterRoutes()

	go node.StartServer()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt)
	<-done

	if err := node.ShutdownServer(time.Second * 5); err != nil {
		logger.Error("HTTP Server shutdown failed", "error", err)
	}
}

func newLogger() *slog.Logger {
	var level slog.Level
	switch config.Config.LogLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
}
