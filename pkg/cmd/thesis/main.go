package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/balits/thesis/pkg/config"
	"github.com/balits/thesis/pkg/service"
	"github.com/balits/thesis/pkg/store"
	"github.com/balits/thesis/pkg/util"
	"github.com/balits/thesis/pkg/web"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		fmt.Printf("Invalid config: %v\n", err)
		os.Exit(1)
	}

	doneCh := make(chan os.Signal, 1)
	signal.Notify(doneCh, os.Interrupt)

	logger := util.NewJSONLogger(config.LogLevel, os.Stdout).With("nodeID", config.ThisService.RaftID)
	httpAddr := config.ThisService.RaftHost + ":" + config.ThisService.InternalHttpPort
	server := web.NewServerWithLogger(httpAddr, logger.With("component", "server"))
	store := store.NewInMemoryStore()
	fsm := service.NewFSM(store)
	svc := service.NewService(store, fsm, server, logger.With("component", "server"), config)
	svc.RegisterRoutes()
	defer svc.Shutdown(time.Second * 5)
	go svc.StartHTTP()

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
