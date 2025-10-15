package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/balits/thesis/config"
	"github.com/balits/thesis/service"
	"github.com/balits/thesis/store"
	"github.com/balits/thesis/web"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		fmt.Printf("Invalid config: %v\n", err)
		os.Exit(1)
	}

	doneCh := make(chan os.Signal, 1)
	signal.Notify(doneCh, os.Interrupt)

	logger := newLogger(config.LogLevel).With("node", config.ThisService.RaftID)
	httpAddr := config.ThisService.RaftHost + ":" + config.ThisService.InternalHttpPort
	server := web.NewServer(httpAddr, web.NewRouter())
	store := store.NewInMemoryStore()
	fsm := service.NewFSM(store)

	svc := service.New(store, fsm, server, logger, config)
	defer svc.Shutdown(time.Second * 5)

	svc.RegisterRoutes()
	startErrorCh := make(chan error, 1)
	svc.StartHTTP(startErrorCh)

	select {
	case err := <-startErrorCh:
		logger.Error("Could not start web server", "error", err)
		return
	case <-time.After(5 * time.Second): // delay
	}
	logger.Info("Web server started")
	//fixme: add an atomic bool "running" to web.Server to quickly check state

	raft, err := svc.NewRaft()
	if err != nil {
		logger.Error("Failed to create raft instance", "error", err)
		return
	}
	svc.Raft = raft
	logger.Info("Raft instance created")

	if config.ThisService.NeedBootstrap {
		logger.Info("Bootstrapping cluster")
		if err = svc.Bootstrap(); err != nil {
			logger.Info("Failed to bootstrapping cluster", "error", err)
			os.Exit(1)
		}
		logger.Info("Bootstrapping cluster successful")
	} else {
		logger.Info("Attempting to join cluster")
		if err = svc.JoinCluster(); err != nil {
			logger.Info("Failed to join cluster", "error", err)
			os.Exit(1)
		}
		logger.Info("Joined cluster successfuly")
	}

	<-doneCh
	logger.Info("Recieved interrupt signal, shutting down...")

	logger.Info("Shutting down HTTP server")
	svc.Shutdown(time.Second * 5)
}

func newLogger(logLevel string) *slog.Logger {
	var level slog.Level
	switch logLevel {
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
		// AddSource: true,
	}))
}
