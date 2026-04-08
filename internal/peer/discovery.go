package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

const k8sHeadlessServiceName = "kave-headless.default.svc.cluster.local"

type DiscoveryMode string

const (
	DiscoveryModeStatic  DiscoveryMode = "static"  // peers are described in a static list or string at startup
	DiscoveryModeDynamic DiscoveryMode = "dynamic" // peers are dynamically resolved based on SRV
)

type DiscoveryOptions struct {
	Mode          DiscoveryMode `json:"mode"`
	Peers         *string       `json:"peers,omitempty"`
	ExpectedCount int           `json:"expected_count"`
}

func (o *DiscoveryOptions) Check() error {
	if o.Mode != DiscoveryModeDynamic && o.Mode != DiscoveryModeStatic {
		return fmt.Errorf("invalid discovery mode: expected %s or %s", DiscoveryModeDynamic, DiscoveryModeStatic)
	}

	if o.Mode == DiscoveryModeStatic && o.Peers == nil {
		return fmt.Errorf("discovery mode set to static, but no peer list provided")
	}

	return nil
}

type DiscoveryService interface {
	// GetPeers returns the current known peers in the cluster,
	// including us.
	GetPeers(context.Context) ([]Peer, error)
	// Me returns the peer information about this node.
	Me() Peer
}

func NewDiscoveryService(me Peer, opts DiscoveryOptions) (d DiscoveryService, err error) {
	switch opts.Mode {
	case DiscoveryModeStatic:
		d, err = newStaticDiscovery(me, opts.Peers)
	case DiscoveryModeDynamic:
		d, err = newDynamicDiscovery(me, opts.ExpectedCount)
	default:
		err = fmt.Errorf("unexpected discovery mode: %s (expected %s or %s)", opts.Mode, DiscoveryModeStatic, DiscoveryModeDynamic)
	}

	if err != nil {
		return nil, fmt.Errorf("discoverService: %w", err)
	}

	return d, err
}

type staticDiscovery struct {
	me    Peer
	peers []Peer
}

func newStaticDiscovery(me Peer, rawPeers *string) (*staticDiscovery, error) {
	var (
		d   = new(staticDiscovery)
		err error
	)
	d.peers, err = parseRawPeers(rawPeers)
	if err != nil {
		return nil, err
	}

	d.me = me
	return d, nil
}

func parseRawPeers(s *string) (peers []Peer, err error) {
	if s == nil {
		return nil, fmt.Errorf("nil raw peer list string")
	}
	str := *s

	if strings.TrimSpace(str) == "" {
		return nil, fmt.Errorf("empty peer list")
	}

	rawPeers := strings.SplitSeq(str, ",")
	for s := range rawPeers {
		parts := strings.SplitN(s, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("failed to parse peer %s (expected id:raft_port:http_port)", s)
		}

		n := Peer{
			NodeID:   parts[0],
			RaftPort: parts[1],
			HttpPort: parts[2],
		}

		if err := n.Check(); err != nil {
			return nil, err
		}
		peers = append(peers, n)
	}

	switch len(peers) {
	case 1, 3, 5:
	default:
		return nil, fmt.Errorf("for optimal raft clusters, cluster size must be 1, 3 or 5, got %d", len(peers))
	}

	return
}

func (d *staticDiscovery) GetPeers(context.Context) ([]Peer, error) { return d.peers, nil }
func (d *staticDiscovery) Me() Peer                                 { return d.me }

// Each StatefulSet pod gets its own SRV record pointing to its stable DNS name:
//
//	kave-voter-0.kave-headless.default.svc.cluster.local:7000
//	kave-voter-1.kave-headless.default.svc.cluster.local:7000
//	kave-voter-2.kave-headless.default.svc.cluster.local:7000
type dnsDiscovery struct {
	// me is the peer information about this node, constructed at
	// startup time from env vars or flags.
	me Peer

	// ServiceName is the DNS name of the headless service, e.g.
	// "kave-headless.default.svc.cluster.local"
	ServiceName string

	// RaftPortName is the SRV service name, e.g. "raft" (matches the port
	// name in the headless Service manifest).
	RaftPortName string

	// HttpPort is used to construct the HTTP address since HTTP doesnt
	// need a separate SRV lookup, its always RaftPorts "neighbour".
	HttpPort string

	// ExpectedCount is how many peers to wait for before proceeding.
	// TODO: set somehow through values.yaml.
	ExpectedCount int

	// PollInterval controls how often we retry the DNS lookup while waiting
	// for all peers to appear.
	PollInterval time.Duration
}

func newDynamicDiscovery(me Peer, expectedCount int) (*dnsDiscovery, error) {
	if expectedCount < 1 {
		return nil, errors.New("expected_count cannot be negative")
	}
	return &dnsDiscovery{
		me:            me,
		ServiceName:   k8sHeadlessServiceName,
		RaftPortName:  "raft",
		ExpectedCount: expectedCount,
		PollInterval:  time.Millisecond * 210,
	}, nil
}

func (d *dnsDiscovery) GetPeers(ctx context.Context) ([]Peer, error) {
	if d.PollInterval == 0 {
		d.PollInterval = 2 * time.Second
	}

	for {
		peers, err := d.lookup()
		if err == nil && len(peers) >= d.ExpectedCount {
			return peers, nil
		}

		found := 0
		if peers != nil {
			found = len(peers)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf(
				"DNS peer discovery timed out waiting for %d peers (found %d): %w",
				d.ExpectedCount, found, ctx.Err(),
			)
		case <-time.After(d.PollInterval):
			// retry
		}
	}
}

func (d *dnsDiscovery) lookup() ([]Peer, error) {
	// LookupSRV returns one record per pod registered under the headless service.
	// Each record contains the pod's stable DNS name and port.
	_, addrs, err := net.LookupSRV(d.RaftPortName, "tcp", d.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("lookup error: SRV lookup for %s failed: %w", d.ServiceName, err)
	}

	// Sort by hostname so every node independently arrives at the same
	// ordered list. This is deterministic bootstrap assignment:
	// the node whose hostname sorts first is always the bootstrapper.
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Target < addrs[j].Target
	})

	peers := make([]Peer, len(addrs))
	for i, addr := range addrs {
		// addr.Target looks like:
		// "kave-voter-0.kave-headless.default.svc.cluster.local."
		// (note the trailing dot which is  standard DNS FQDN format)
		hostname := strings.TrimSuffix(addr.Target, ".")

		// The NodeID is derived from the pod name portion of the FQDN,
		// eg.: "kave-voter-0". This matches what K8s uses as the pod name
		// and what your Raft stable store uses as the server ID.
		nodeID := strings.Split(hostname, ".")[0]

		peers[i] = Peer{
			NodeID:   nodeID,
			Hostname: hostname,
			RaftPort: fmt.Sprintf("%d", addr.Port),
			HttpPort: d.HttpPort,
		}
	}

	return peers, nil
}

// Me identifies which peer in the list is this node, using the POD_NAME
// environment variable that K8s injects via the downward API.
// This is more reliable than enumerating network interfaces in K8s, where
// interface names are less predictable than in a bare-metal environment.
func (d *dnsDiscovery) Me() Peer {
	return d.me
}

// ShouldBootstrap returns true for the first peer in the sorted list.
// Because every node sorts the SRV records identically, exactly one node
// will return true — whichever has the lexicographically lowest hostname.
// In a StatefulSet this is always the pod with ordinal 0 (e.g. kave-voter-0).
func (d *dnsDiscovery) ShouldBootstrap(peers []Peer) bool {
	if len(peers) == 0 {
		return false
	}
	return peers[0].NodeID == d.me.NodeID
}
