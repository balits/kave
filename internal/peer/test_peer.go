package peer

import "testing"

func TestPeer() Peer {
	if !testing.Testing() {
		panic("peer.TestPeer() is only allowed in tests!")
	}

	return Peer{
		NodeID:   "test_peer",
		Hostname: "0.0.0.0",
		RaftPort: "7000",
		HttpPort: "8000",
	}
}
