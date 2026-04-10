package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/transport"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
)

var (
	ErrLeaderNotFound         = errors.New("raft service: leader not found")
	ErrDoublePeerRegistration = errors.New("raft service: attempted to register peers twice")
)

type RaftService interface {
	// RegisterPeers build the internal peer map of the service
	// from the given peer list. It returns an error if the map
	// is already initialized.
	RegisterPeers(peers []peer.Peer) error

	// Bootstrap boostraps the whole cluster, with "me" as the only node in it.
	Bootstrap(ctx context.Context, me peer.Peer) error
	// JoinCluster sends join requests to each known peer, or the leader
	// if we get any additional information about it, using exponential backoff.
	JoinCluster(ctx context.Context, me peer.Peer) error
	// AddToCluster parses the request and extracts the node information, then adds
	// it to the cluster. It first verifies we are still the leader,
	// returning an error if not.
	AddToCluster(ctx context.Context, req transport.JoinRequest) error

	// Leader returns the current leader, or ErrLeaderNotFound
	Leader() (peer.Peer, error)
	Stats() map[string]string               // Returns the raft libraries internal statistics
	RaftState() raft.RaftState              // Our current state
	VerifyLeader(ctx context.Context) error // Verify that we are the leader, or return an error if not
	LaggingBehind() error                   // Check if we are lagging behind the leader, and return an error if we are
}

type raftSvc struct {
	peerMu       sync.RWMutex
	peerMap      map[string]peer.Peer
	r            *raft.Raft
	lagThreshold uint64
	logger       *slog.Logger
}

func NewRaftService(logger *slog.Logger, r *raft.Raft, lagThreshold uint64) RaftService {
	rs := &raftSvc{
		// peerDiscovery needs a context, so we only call it in node.Run(ctx), not at nodes construction time
		peerMap:      nil,
		r:            r,
		lagThreshold: lagThreshold,
		logger:       logger.With("component", "raft_service"),
	}

	rs.logger.Info("RaftService instantiated", "peer_count", len(rs.peerMap))

	return rs
}

func (rs *raftSvc) RegisterPeers(peers []peer.Peer) error {
	rs.peerMu.Lock()
	defer rs.peerMu.Unlock()
	if rs.peerMap != nil {
		return ErrDoublePeerRegistration
	}

	pm := make(map[string]peer.Peer)
	for _, p := range peers {
		pm[string(p.NodeID)] = p
	}
	rs.peerMap = pm
	return nil
}

func (rs *raftSvc) Bootstrap(ctx context.Context, me peer.Peer) error {
	rs.logger.Info("Bootstrapping cluster")
	// with only this node in the configuration (no peers)
	// bootstrapping the cluster will immediatly elect this node to leader,
	// without wasting time with heartbeats to other nodes
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       raft.ServerID(me.NodeID),
				Address:  me.GetRaftAddress(),
				Suffrage: raft.Voter,
			},
		},
	}
	return util.WaitFuture(ctx, rs.r.BootstrapCluster(cfg))
}

func (rs *raftSvc) AddToCluster(ctx context.Context, req transport.JoinRequest) error {
	rs.logger.WithGroup("peer").
		Info("Adding peer to cluster",
			"id", req.Peer.NodeID,
			"hostname", req.Peer.Hostname,
			"raft_addr", req.Peer.GetRaftAddress(),
		)

	// fix todo: do we need a round trip here? AddVoter already fails with Not Leader
	// if err := util.WaitFuture(ctx, rs.r.VerifyLeader()); err != nil {
	// 	return fmt.Errorf("failed to add peer to cluster: %v", err)
	// }

	configFut := rs.r.GetConfiguration()
	if err := util.WaitFuture(ctx, configFut); err != nil {
		return fmt.Errorf("failed to add peer to cluster: %v", err)
	}

	raftConfig := configFut.Configuration()
	for _, node := range raftConfig.Servers {
		if string(node.ID) == req.Peer.NodeID {
			rs.logger.WithGroup("peer").
				Info("Peer is already a member",
					"id", req.Peer.NodeID,
					"hostname", req.Peer.Hostname,
					"raft_addr", req.Peer.GetRaftAddress(),
				)
			return nil
		}
	}

	addFut := rs.r.AddVoter(raft.ServerID(req.Peer.NodeID), req.Peer.GetRaftAddress(), 0, 5*time.Second)
	if err := util.WaitFuture(ctx, addFut); err != nil {
		return fmt.Errorf("failed to add peer to cluster: %v", err)
	}

	rs.peerMu.Lock()
	rs.peerMap[req.Peer.NodeID] = req.Peer
	rs.peerMu.Unlock()

	rs.logger.WithGroup("peer").
		Info("Peer added successfully",
			"id", req.Peer.NodeID,
			"hostname", req.Peer.Hostname,
			"raft_addr", req.Peer.GetRaftAddress(),
		)
	return nil
}

func (rs *raftSvc) JoinCluster(ctx context.Context, me peer.Peer) error {
	var urls []string
	for _, p := range rs.peerMap {
		if p.NodeID == me.NodeID {
			continue
		}
		urls = append(urls, p.HttpURL()+transport.RouteCluster+"/join")
	}
	rs.logger.Info("Attempting to join cluster", "peers", urls)

	body := transport.JoinRequest{Peer: me}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to serialize request body: %v", body)
	}

	initialTimeout := time.Second * 4 // slightly higher in k8s than in compose
	jitter := time.Duration(rand.Int64N(int64(initialTimeout)))
	time.Sleep(initialTimeout + jitter) //sleep so that bootstrapping node has some time to elect itself
	if err := rs.joinWithBackoff(ctx, urls, 4, jsonBody); err != nil {
		return err
	}

	rs.logger.Info("Joined cluster successfuly")

	return nil
}

func (rs *raftSvc) joinWithBackoff(ctx context.Context, urls []string, attempts int, jsonBody []byte) error {
	var lastError error
	for a := range attempts {
		l := rs.logger.With("attempt", a)
		for _, url := range urls {
			err := join(ctx, url, jsonBody)
			if err == nil {
				return nil // ok
			} else {
				lastError = err
			}
			l.Debug("Attempting to join cluster", "url", url, "attempt", a, "error", err)
		}

		l.Info("taking a random sleep between join attempts")
		backoff := (2 << a) * time.Second
		jitter := time.Duration(rand.Int64N(1000)) * time.Millisecond
		time.Sleep(backoff + time.Duration(jitter)/2)
	}

	return fmt.Errorf("could not join peers after %d attempts, last error: %v", attempts, lastError)
}

func join(ctx context.Context, url string, jsonBody []byte) error {
	// dip early if context was canceled between attempts
	if ctx.Err() != nil {
		return ctx.Err()
	}

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
		return join(ctx, loc, jsonBody)
	default:
		return fmt.Errorf("status %s", res.Status)
	}
}

func (rs *raftSvc) Leader() (peer.Peer, error) {
	_, i := rs.r.LeaderWithID()
	rs.peerMu.RLock()
	defer rs.peerMu.RUnlock()
	p, ok := rs.peerMap[string(i)]
	if !ok {
		return peer.Peer{}, ErrLeaderNotFound
	}
	return p, nil
}

func (rs *raftSvc) Stats() map[string]string {
	s := rs.r.Stats()
	addr, id := rs.r.LeaderWithID()
	s["leader_addr"] = string(addr)
	s["leader_id"] = string(id)
	return s
}

func (rs *raftSvc) RaftState() raft.RaftState {
	return rs.r.State()
}

func (rs *raftSvc) VerifyLeader(ctx context.Context) error {
	return util.WaitFuture(ctx, rs.r.VerifyLeader())
}

func (rs *raftSvc) LaggingBehind() error {
	c, a := rs.r.CommitIndex(), rs.r.AppliedIndex()
	if c-a > rs.lagThreshold {
		return fmt.Errorf("raft is lagging behind: commit index %d, applied index %d", c, a)
	}
	return nil
}
