package raftnode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (n *Node) Start() error {
	if n.Raft == nil {
		return errors.New("raft not initialized")
	}

	hasState, err := raft.HasExistingState(n.Logs, n.Stable, n.Snapshots)
	if err != nil {
		return fmt.Errorf("failed reading raft state: %v", err)
	}

	if hasState {
		n.Logger.Info("Exsisting Raft state found; resuming cluster participation")
		// Raft will recover terms, logs etc
		return nil
	}

	if n.Config.Bootstrap {
		if err := n.BootstrapCluster(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
		n.Logger.Info("Bootstrapped cluster successfuly")
	} else {
		var urls []string
		for _, p := range n.Config.Peers {
			if p.NodeID == n.Config.NodeID {
				continue
			}
			urls = append(urls, "http://"+p.GetInternalHttpAddress()+"/join")
		}

		initialTimeout := time.Second * 2
		jitter := time.Duration(rand.Int64N(int64(initialTimeout)))
		time.Sleep(initialTimeout + jitter/2) //sleep so that bootstrapping node has some time to elect itself
		if err := JoinWithBackoff(n.Config.Peer, urls, 5, n.Logger); err != nil {
			return fmt.Errorf("failed to join cluster: %v", err)
		}
		n.Logger.Info("Joined cluster successfuly")
	}

	return nil
}

func (n *Node) BootstrapCluster() error {
	// with only this node in the configuration (no peers)
	// bootstrapping the cluster will immediatly elect this node to leader,
	// without wasting time with heartbeats to other nodes
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       raft.ServerID(n.Config.NodeID),
				Address:  n.Config.GetRaftAddress(),
				Suffrage: raft.Voter,
			},
		},
	}
	return n.Raft.BootstrapCluster(cfg).Error()
}

func JoinWithBackoff(me config.Peer, urls []string, attempts int, logger *slog.Logger) error {
	logger.Debug("Attempting to join cluster", "peers", urls)

	body := web.JoinBody{
		ID:   me.NodeID,
		Addr: string(me.GetRaftAddress()),
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
				logger.Debug("Attempting successful", "url", url, "body", body, "attempt", a)
				return nil // ok
			} else {
				logger.Debug("Attempt failed", "url", url, "attempt", a+1, "error", err)
				lastError = err
			}
		}

		backoff := (2 << a) * time.Second
		jitter := rand.Int64N(1000) * time.Hour.Milliseconds()
		time.Sleep(backoff + time.Duration(jitter)/2)
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
	msg := sb.String()

	switch res.StatusCode {
	case 200, 204: // succesful join, already joined
		return nil
	case http.StatusTemporaryRedirect: // redirection to leaders address
		loc := res.Header.Get("Location")
		if loc == "" {
			return fmt.Errorf("redirected without location, status: %s", res.Status)
		}
		return join(loc, jsonBody)
	case http.StatusConflict, http.StatusBadRequest: // node wasnt the leader or bad request
		return fmt.Errorf("%s, status:  %s", msg, res.Status)
	case http.StatusServiceUnavailable: // no leader elected yet
		return fmt.Errorf("%s, status: %s", msg, res.Status)
	case http.StatusInternalServerError: // no leader elected yet
		return fmt.Errorf("%s, status: %s", msg, res.Status)
	default:
		return fmt.Errorf("unexpected status %s, error: %s", res.Status, msg)
	}
}
