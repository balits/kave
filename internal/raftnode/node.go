// package node implements a single raft node with an http server and a key-value store
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

type Node struct {
	// NodeEnv provides us with important dependencies like stores, fsm or logger
	NodeEnv

	// Raft is the pointer to Raft instance
	Raft *raft.Raft
}

func CreateNode(env *NodeEnv) *Node {
	return &Node{
		Raft:    nil,
		NodeEnv: *env,
	}
}

func (n *Node) SetupRaft() error {
	if n.Raft != nil {
		return fmt.Errorf("attemted to setup raft twice")
	}
	r, err := raft.NewRaft(n.RaftConfig, n.fsm.(raft.FSM), n.Logs, n.Stable, n.Snapshots, n.Transport)
	if err != nil {
		return fmt.Errorf("failed to setup raft: %v", err)
	}
	n.Raft = r
	return nil
}

func (n *Node) Run(ctx context.Context) error {
	done := make(chan struct{}, 1)
	errorCh := make(chan error, 1)
	go func() {
		if err := n.SetupRaft(); err != nil {
			errorCh <- err
		}
		if err := n.Start(); err != nil {
			errorCh <- err
		}
		<-done
	}()

	select {
	case <-ctx.Done():
		close(done)
		return n.Shutdown(10 * time.Second)
	case err := <-errorCh:
		close(done)
		n.Logger.Error("Error during startup, shutting down node", "error", err)
		return errors.Join(err, n.Shutdown(10*time.Second))
	}
}

// Shutdown terminates both the http server with the supplied timeout and the raft node
func (n *Node) Shutdown(timeout time.Duration) error {
	if n.Raft == nil {
		return nil
	}
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		n.Logger.Info("Shuting down node")
		if err := n.Raft.Shutdown().Error(); err != nil {
			errorCh <- err
		}
		errorCh <- nil
	}()

	select {
	case <-ctx.Done():
		return errors.New("node shutdown timed out")
	case err := <-errorCh:
		return err
	}
}

func (n *Node) ToString() string {
	raftId := n.Config.NodeID
	raftAddr := n.Config.GetRaftAddress()
	httpInt := n.Config.GetInternalHttpAddress()
	return fmt.Sprintf("%s(%s, %s)", raftId, raftAddr, httpInt)
}
