package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/balits/kave/internal/common"
	"github.com/balits/kave/internal/config"
	"github.com/hashicorp/raft"
)

// ClusterService az interfész, amely a klaszter műveleteket definiálja.
// Ez a szolgáltatás segít a klaszterhez való csatlakozásban, a klaszterből való kilépésben, és a klaszter állapotának lekérdezésében.
type ClusterService interface {
	Bootstrap(ctx context.Context) error
	JoinCluster(ctx context.Context, peers map[string]common.Peer) error
	AddToCluster(ctx context.Context, req common.JoinRequest) error

	Stats() (map[string]string, error)
	Meta() common.Meta
}

func NewClusterService(raft *raft.Raft, cfg *config.Config, logger *slog.Logger) ClusterService {
	return &clusterService{
		raft:   raft,
		logger: logger.With("component", "cluster-service"),
		me:     cfg.Me,
	}
}

type clusterService struct {
	raft   *raft.Raft
	logger *slog.Logger
	me     common.Peer
}

func (s *clusterService) Bootstrap(ctx context.Context) error {
	s.logger.Info("Bootstrapping cluster")
	// with only this node in the configuration (no peers)
	// bootstrapping the cluster will immediatly elect this node to leader,
	// without wasting time with heartbeats to other nodes
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       raft.ServerID(s.me.NodeID),
				Address:  s.me.GetRaftAddress(),
				Suffrage: raft.Voter,
			},
		},
	}
	return s.raft.BootstrapCluster(cfg).Error()
}

func (s *clusterService) AddToCluster(ctx context.Context, req common.JoinRequest) error {
	//TODO Info -> Debug
	s.logger.Info("Adding peer to cluster", "peer_id", req.NodeID, "peer_addr", req.RaftAddr)

	if err := waitFuture(ctx, s.raft.VerifyLeader()); err != nil {
		return fmt.Errorf("failed to add peer to cluster: %v", err)
	}

	fut := s.raft.GetConfiguration()

	if err := waitFuture(ctx, fut); err != nil {
		return fmt.Errorf("failed to add peer to cluster: %v", err)
	}

	raftConfig := fut.Configuration()
	for _, node := range raftConfig.Servers {
		if node.ID == req.NodeID {
			s.logger.Info("Peer is already a member", "peer_id", req.NodeID, "peer_addr", req.RaftAddr)
			return nil // already memeber
		}
	}

	fut2 := s.raft.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 5*time.Second)
	if err := waitFuture(ctx, fut2); err != nil {
		return fmt.Errorf("failed to add peer to cluster: %v", err)
	}
	s.logger.Info("Peer added successfully", "peer_id", req.NodeID, "peer_addr", req.RaftAddr)
	return nil
}

func (s *clusterService) JoinCluster(ctx context.Context, peers map[string]common.Peer) error {
	var urls []string
	for _, p := range peers {
		urls = append(urls, "http://"+p.GetInternalHttpAddress()+common.UriCluster+"/join")
	}
	s.logger.Info("Attempting to join cluster", "peers", urls)

	body := common.JoinRequest{
		NodeID:   raft.ServerID(s.me.NodeID),
		RaftAddr: s.me.GetRaftAddress(),
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to serialize request body: %v", body)
	}

	initialTimeout := time.Second * 1
	jitter := time.Duration(rand.Int64N(int64(initialTimeout)))
	time.Sleep(initialTimeout + jitter/2) //sleep so that bootstrapping node has some time to elect itself
	if err := s.joinWithBackoff(urls, 4, jsonBody); err != nil {
		return err
	}

	s.logger.Info("Joined cluster successfuly")

	return nil
}

func (s *clusterService) Stats() (map[string]string, error) {
	return s.raft.Stats(), nil
}

func (s *clusterService) Meta() common.Meta {
	leaderID, _ := s.raft.LeaderWithID()
	return common.Meta{
		ApplyIndex: s.raft.AppliedIndex(),
		LastIndex:  s.raft.LastIndex(),
		Term:       s.raft.CurrentTerm(),
		LeaderID:   string(leaderID),
	}
}

func (s *clusterService) joinWithBackoff(urls []string, attempts int, jsonBody []byte) error {
	var lastError error
	for a := range attempts {
		for _, url := range urls {
			err := join(url, jsonBody)
			if err == nil {
				return nil // ok
			} else {
				lastError = err
			}
			s.logger.Debug("Attempting to join cluster", "url", url, "attempt", a, "error", err)
		}

		backoff := (2 << a) * time.Second
		jitter := rand.Int64N(1000) * time.Hour.Milliseconds()
		time.Sleep(backoff + time.Duration(jitter)/2)
	}

	return fmt.Errorf("could not join peers after %d attempts, last error: %v", attempts, lastError)
}

func join(url string, jsonBody []byte) error {
	body := io.NopCloser(bytes.NewReader(jsonBody))
	res, err := http.DefaultClient.Post(url, "application/json", body)
	if err != nil {
		return fmt.Errorf("client error: %v", err)
	}

	switch res.StatusCode {
	case 200, 204: // succesful join, already joined
		return nil
	case http.StatusTemporaryRedirect: // redirection to leaders address
		loc := res.Header.Get("Location")
		if loc == "" {
			return fmt.Errorf("redirected without location, status: %s", res.Status)
		}
		return join(loc, jsonBody)
	default:
		return fmt.Errorf("status %s", res.Status)
	}
}
