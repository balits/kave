package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/balits/thesis/internal/api"
	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/raftnode"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/util"
)

func main() {
	config, err := config.LoadConfig()
	check(err, nil)

	logger := util.NewJSONLogger(config.LogLevel, os.Stdout)
	fsmstore := store.NewInMemoryStore() // add if branch on config.InMemory | config.Persistence
	raftStores, err := raftnode.LoadRaftStores(config)
	check(err, logger)

	node, err := raftnode.NewNode(config, fsmstore, raftStores, logger.With("component", "raftnode"))
	check(err, node.Logger)

	httpAddr := config.ThisService.GetInternalHttpAddress()
	server := api.NewServer(httpAddr, node, logger.With("component", "httpserver"))
	server.RegisterRoutes()
	go server.Start()

	defer func() {
		node.Shutdown(5 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	err = node.BootstrapOrJoinCluster()
	check(err, node.Logger)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt)
	<-done
}

func check(err error, logger *slog.Logger) {
	if err != nil {
		if logger != nil {
			logger.Error("Fatal error occured", "error", err)
		} else {
			fmt.Println("Fatal error occured: ", err)
		}
		os.Exit(1)
	}
}
