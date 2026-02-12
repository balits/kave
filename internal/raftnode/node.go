// package node implements a single raft node with an http server and a key-value store
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/balits/thesis/internal/metrics"
	"github.com/hashicorp/raft"
)

type Node struct {
	// NodeEnv provides us with important dependencies like stores, fsm or logger
	NodeEnv

	// Raft is the pointer to Raft instance
	Raft *raft.Raft

	// Information about the state of the Node
	nodeMetrics metrics.NodeMetricsAtomic
}

func CreateNode(env *NodeEnv) *Node {
	nodeMetrics := new(metrics.NodeMetricsAtomic)
	nodeMetrics.StorageState.Store(uint32(metrics.StorageStateOperational))
	return &Node{
		Raft:        nil,
		NodeEnv:     *env,
		nodeMetrics: *nodeMetrics,
	}
}

func (n *Node) SetupRaft() error {
	if n.Raft != nil {
		return fmt.Errorf("attemted to setup raft twice")
	}
	r, err := raft.NewRaft(n.RaftConfig, n.fsm, n.Logs, n.Stable, n.Snapshots, n.Transport)
	if err != nil {
		return fmt.Errorf("failed to setup raft: %v", err)
	}
	n.Raft = r

	n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateInit))
	return nil
}

func (n *Node) Run(ctx context.Context) error {
	done := make(chan struct{}, 1)
	errorCh := make(chan error, 1)
	go func() {
		if err := n.SetupRaft(); err != nil {
			n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateSetupFailed))
			errorCh <- err
		} else {
			n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateInit))
		}

		if err := n.Start(); err != nil {
			n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateStartFailed))
			errorCh <- err
		} else {
			n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateJoined))
		}

		<-done
	}()

	select {
	case <-ctx.Done():
		close(done)
		err := n.Shutdown(10 * time.Second)
		if err != nil {
			n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateShutdownFailed))
		}
		return err
	case err := <-errorCh:
		n.nodeMetrics.LastNodeStateError.Store(err)
		close(done)
		err = errors.Join(err, n.Shutdown(10*time.Second))
		if err != nil {
			n.nodeMetrics.NodeState.Store(uint32(metrics.NodeStateShutdownFailed))
			n.nodeMetrics.LastNodeStateError.Store(err)
		}
		return err
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

// ========= metrics.NodeMetricsProvider impl =========

func (n *Node) NodeMetrics() *metrics.NodeMetrics {
	return n.nodeMetrics.NodeMetrics()
}

// ========= metrics.StorageMetricsProvider impl =========

func (n *Node) StorageMetrics() *metrics.StorageMetrics {
	return n.fsm.Store.StorageMetrics()
}

// ========= metrics.FsmMetricsProvider impl =========

func (n *Node) FsmMetrics() *metrics.FsmMetrics {
	return n.fsm.FsmMetrics()
}
