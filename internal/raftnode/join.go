package raftnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (n *Node) BootstrapOrJoinCluster() error {
	err := n.bootstrapOrJoinCluster()
	n.joinedState.Store(err == nil)
	return err
}

func (n *Node) bootstrapOrJoinCluster() error {
	hasState, err := raft.HasExistingState(n.RaftStores.LogStore, n.RaftStores.StableStore, n.RaftStores.SnapshotStore)
	if err != nil {
		return fmt.Errorf("failed reading raft state: %v", err)
	}

	if hasState {
		n.Logger.Info("Exsisting Raft state found; resuming cluster participation")
		// Raft will recover terms, logs etc
		return nil
	}

	if n.Config.ThisService.NeedBootstrap {
		if err := n.bootstrap(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
		n.Logger.Info("Bootstrapped cluster successfuly")
	} else {
		if err := n.joinCluster(); err != nil {
			return fmt.Errorf("failed to join cluster: %v", err)
		}
		n.Logger.Info("Joined cluster successfuly")
	}

	return nil
}

// bootstrap tries to
func (n *Node) bootstrap() error {
	// with only this node in the configuration (no peers)
	// bootstrapping the cluster will immediatly elect this node to leader,
	// without wasting time with heartbeats to other nodes
	clusterConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.Config.ThisService.RaftID),
				Address: raft.ServerAddress(n.Config.ThisService.GetRaftAddress()),
			},
		},
	}
	return n.Raft.BootstrapCluster(clusterConfig).Error()
}

func (n *Node) joinCluster() error {
	me := n.Config.ThisService
	var urls []string
	for _, i := range n.Config.ClusterInfo {
		if i == *me {
			continue
		}
		urls = append(urls, "http://"+i.InternalHost+":"+i.InternalHttpPort+"/join")
	}
	n.Logger.Debug("Attempting to join cluster", "peers", urls)
	if err := joinWithBackoff(me, urls, 4, n.Logger); err != nil {
		n.Logger.Error("Joining cluster failed", "error", err)
		return err
	}
	return nil
}

func joinWithBackoff(me *config.ServiceInfo, urls []string, attempts int, logger *slog.Logger) error {
	body := web.JoinBody{
		ID:   me.RaftID,
		Addr: me.GetRaftAddress(),
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to serialize request body: %v", body)
	}

	var lastError error
	for a := range attempts {
		for _, url := range urls {
			err := join(url, jsonBody)
			logger.Debug("Attempting to join cluster", "url", url, "body", body, "attempt", a)
			if err == nil {
				return nil // ok
			} else {
				lastError = err
			}
		}
		time.Sleep(time.Duration(2<<a) * time.Second)
	}

	return fmt.Errorf("could not join peers after %d attempts, last error: %v", attempts, lastError)
}

func join(url string, jsonBody []byte) error {
	body := bytes.NewReader(jsonBody)
	res, err := http.DefaultClient.Post(url, "application/json", body)
	if err != nil {
		return fmt.Errorf("client error: %v", err)
	}
	defer res.Body.Close()
	sb := new(strings.Builder)
	_, err = io.Copy(sb, res.Body)
	if err != nil {
		return fmt.Errorf("client error: %v", err)
	}
	errMsg := sb.String()

	switch res.StatusCode {
	case 200, 204:
		// succesful join, already joined
		return nil
	case http.StatusConflict, http.StatusBadRequest:
		// node wasnt the leader or bad request
		return fmt.Errorf("status: %s, %s", res.Status, errMsg)
	default:
		return fmt.Errorf("status: %s, unexpected status code, error: %s", res.Status, errMsg)
	}
}
