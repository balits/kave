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
	"strconv"
	"sync"
	"time"

	"github.com/balits/kave/internal/mvcc"
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
	JoinCluster(ctx context.Context, me peer.Peer, adminToken string) error
	// AddToCluster parses the request and extracts the node information, then adds
	// it to the cluster. It first verifies we are still the leader,
	// returning an error if not.
	AddToCluster(ctx context.Context, req transport.JoinRequest) error

	// Leader returns the current leader, or ErrLeaderNotFound
	Leader(ctx context.Context) (peer.Peer, error)
	Peers() []peer.Peer
	RaftConfiguration(ctx context.Context) (raft.Configuration, error)
	Stats(ctx context.Context) map[string]string // Returns the raft libraries internal statistics, extended with some of our keys
	RaftState() raft.RaftState                   // Our current state
	VerifyLeader(ctx context.Context) error      // Verify that we are the leader, or return an error if not
	LaggingBehind() error                        // Check if we are lagging behind the leader, and return an error if we are
}

type raftSvc struct {
	peerMu       sync.RWMutex
	peerMap      map[string]peer.Peer
	r            *raft.Raft
	lagThreshold uint64
	store        mvcc.StoreMetaReader
	logger       *slog.Logger
}

func NewRaftService(logger *slog.Logger, r *raft.Raft, lagThreshold uint64, store mvcc.StoreMetaReader) RaftService {
	rs := &raftSvc{
		// peerDiscovery needs a context, so we only call it in node.Run(ctx), not at nodes construction time
		peerMap:      nil,
		r:            r,
		lagThreshold: lagThreshold,
		store:        store,
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
			if node.Address == req.Peer.GetRaftAddress() {
				rs.logger.WithGroup("peer").
					Info("Peer is already a member",
						"id", req.Peer.NodeID,
						"raft_addr", req.Peer.GetRaftAddress(),
					)
				return nil
			}
			// same id, different address -> AddVoter update it
			rs.logger.WithGroup("peer").
				Info("Peer re-joining with new address",
					"id", req.Peer.NodeID,
					"old_addr", node.Address,
					"new_addr", req.Peer.GetRaftAddress(),
				)
			break
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

func (rs *raftSvc) JoinCluster(ctx context.Context, me peer.Peer, adminToken string) error {
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

	initialTimeout := time.Second * 2 // slightly higher in k8s than in compose | or js attempt one more time hehe
	jitter := time.Duration(rand.Int64N(int64(initialTimeout)))
	time.Sleep(initialTimeout + jitter) //sleep so that bootstrapping node has some time to elect itself
	if err := rs.joinWithBackoff(ctx, urls, 5, jsonBody, adminToken); err != nil {
		return err
	}

	rs.logger.Info("Joined cluster successfuly")

	return nil
}

func (rs *raftSvc) joinWithBackoff(ctx context.Context, urls []string, attempts int, jsonBody []byte, adminToken string) error {
	var lastError error
	for a := range attempts {
		l := rs.logger.With("attempt", a)
		for _, url := range urls {
			err := join(ctx, url, jsonBody, adminToken)
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

func join(ctx context.Context, url string, jsonBody []byte, adminToken string) error {
	// dip early if context was canceled between attempts
	if ctx.Err() != nil {
		return ctx.Err()
	}

	body := io.NopCloser(bytes.NewReader(jsonBody))
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("client error: %v", err)
	}
	req.Header.Set(transport.AdminAuthTokenHeaderName, adminToken)

	res, err := http.DefaultClient.Do(req)
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
		return join(ctx, loc, jsonBody, adminToken)
	default:
		return fmt.Errorf("status %s", res.Status)
	}
}

func (rs *raftSvc) Leader(ctx context.Context) (peer.Peer, error) {
	addr, id := rs.r.LeaderWithID()
	if len(addr) == 0 || len(id) == 0 {
		return peer.Peer{}, ErrLeaderNotFound
	}

	conf, err := rs.RaftConfiguration(ctx)
	if err != nil {
		return peer.Peer{}, err
	}

	var foundLeader bool
	for _, srv := range conf.Servers {
		if srv.ID == id {
			foundLeader = true
			break
		}
	}

	rs.peerMu.RLock()
	defer rs.peerMu.RUnlock()

	if foundLeader {
		p, ok := rs.peerMap[string(id)]
		if !ok {
			return peer.Peer{}, fmt.Errorf("%w: new unkown leader found (not part of discovered peers)", ErrLeaderNotFound)
		}
		return p, nil
	}

	return peer.Peer{}, ErrLeaderNotFound
}

func (rs *raftSvc) Peers() (out []peer.Peer) {
	rs.peerMu.RLock()
	defer rs.peerMu.RUnlock()
	for _, p := range rs.peerMap {
		out = append(out, peer.Peer{
			NodeID:   p.NodeID,
			Hostname: p.Hostname,
			RaftPort: p.RaftPort,
			HttpPort: p.HttpPort,
		})
	}
	return out
}

func (rs *raftSvc) RaftConfiguration(ctx context.Context) (cfg raft.Configuration, err error) {
	fut := rs.r.GetConfiguration()
	if err = util.WaitFuture(ctx, fut); err != nil {
		return
	}
	cfg = fut.Configuration()
	return
}

func (rs *raftSvc) Stats(ctx context.Context) map[string]string {
	s := rs.r.Stats()
	addr, id := rs.r.LeaderWithID()
	s["leader_addr"] = string(addr)
	s["leader_id"] = string(id)

	rev, compactedRev := rs.store.Revisions()
	s["revision"] = strconv.FormatInt(rev.Main, 10)
	s["compacted_revision"] = strconv.FormatInt(compactedRev, 10)

	conf, err := rs.RaftConfiguration(ctx)
	if err != nil {
		s["readable_configuration"] = "[]"
		return s
	}

	// configPeer mirrors of raft.Server
	// but makes it readable to clients
	// who rely on json parsing (frontend ui)
	type configPeer struct {
		Suffrage string `json:"suffrage"`
		ID       string `json:"id"`
		Address  string `json:"address"`
	}

	var peers []configPeer
	for _, s := range conf.Servers {
		peers = append(peers, configPeer{
			Suffrage: s.Suffrage.String(),
			ID:       string(s.ID),
			Address:  string(s.Address),
		})
	}

	peersBytes, err := json.Marshal(peers)
	if err != nil {
		s["readable_configuration"] = "[]"
		return s
	}

	s["readable_configuration"] = string(peersBytes)
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

	if a == 0 {
		addr, _ := rs.r.LeaderWithID()
		if addr != "" {
			rs.logger.Warn("node lagging behind",
				"cause", "node has not applied any state (waiting for InstallRPC / initial log sync)",
			)
			return fmt.Errorf("node has not applied any state (waiting for InstallRPC / initial log sync)")
		}
	}

	if c-a > rs.lagThreshold {
		return fmt.Errorf("raft is lagging behind: commit index %d, applied index %d", c, a)
	}
	return nil
}
