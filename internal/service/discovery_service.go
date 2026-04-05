package service

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/balits/kave/internal/config"
)

// Each StatefulSet pod gets its own SRV record pointing to its stable DNS name:
//
//	kave-voter-0.kave-headless.default.svc.cluster.local:7000
//	kave-voter-1.kave-headless.default.svc.cluster.local:7000
//	kave-voter-2.kave-headless.default.svc.cluster.local:7000
type DNS struct {
	// Service is the DNS name of the headless service, e.g.
	// "kave-headless.default.svc.cluster.local"
	Service string

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

func (d *DNS) Peers(ctx context.Context) ([]config.Peer, error) {
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

func (d *DNS) lookup() ([]config.Peer, error) {
	// LookupSRV returns one record per pod registered under the headless service.
	// Each record contains the pod's stable DNS name and port.
	_, addrs, err := net.LookupSRV(d.RaftPortName, "tcp", d.Service)
	if err != nil {
		return nil, fmt.Errorf("lookup error: SRV lookup for %s failed: %w", d.Service, err)
	}

	// Sort by hostname so every node independently arrives at the same
	// ordered list. This is deterministic bootstrap assignment:
	// the node whose hostname sorts first is always the bootstrapper.
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Target < addrs[j].Target
	})

	peers := make([]config.Peer, len(addrs))
	for i, addr := range addrs {
		// addr.Target looks like:
		// "kave-voter-0.kave-headless.default.svc.cluster.local."
		// (note the trailing dot which is  standard DNS FQDN format)
		hostname := strings.TrimSuffix(addr.Target, ".")

		// The NodeID is derived from the pod name portion of the FQDN,
		// eg.: "kave-voter-0". This matches what K8s uses as the pod name
		// and what your Raft stable store uses as the server ID.
		nodeID := strings.Split(hostname, ".")[0]

		peers[i] = config.Peer{
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
func (d *DNS) Me(peers []config.Peer) (config.Peer, error) {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		// Fallback for non-K8s environments: enumerate network interfaces
		// and find which peer IP matches one of our own.
		return meByIP(peers)
	}

	for _, peer := range peers {
		if peer.NodeID == podName {
			return peer, nil
		}
	}

	return config.Peer{}, fmt.Errorf(
		"DNS discovery error: Me() failed: POD_NAME=%q not found in peer list %v", podName, peers,
	)
}

// ShouldBootstrap returns true for the first peer in the sorted list.
// Because every node sorts the SRV records identically, exactly one node
// will return true — whichever has the lexicographically lowest hostname.
// In a StatefulSet this is always the pod with ordinal 0 (e.g. kave-voter-0).
func (d *DNS) ShouldBootstrap(me config.Peer, peers []config.Peer) bool {
	if len(peers) == 0 {
		return false
	}
	return peers[0].NodeID == me.NodeID
}

// meByIP is a fallback for non-K8s environments (e.g. Docker Compose)
// where POD_NAME is not set. It enumerates local network interfaces and
// finds which peer's hostname resolves to one of our own IPs.
func meByIP(peers []config.Peer) (config.Peer, error) {
	localIPs, err := localIPSet()
	if err != nil {
		return config.Peer{}, fmt.Errorf("failed to enumerate local IPs: %w", err)
	}

	for _, peer := range peers {
		addrs, err := net.LookupHost(peer.Hostname)
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			if localIPs[addr] {
				return peer, nil
			}
		}
	}

	return config.Peer{}, fmt.Errorf(
		"could not identify self in peer list %v (local IPs: %v)", peers, localIPs,
	)
}

func localIPSet() (map[string]bool, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	ips := make(map[string]bool)
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ips[v.IP.String()] = true
			case *net.IPAddr:
				ips[v.IP.String()] = true
			}
		}
	}
	return ips, nil
}
