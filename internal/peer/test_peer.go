package peer

import "testing"

func TestPeer() Peer {
	if !testing.Testing() {
		panic("peer.TestPeer() is only allowed in tests!")
	}

	return Peer{
		NodeID:   "test_peer",
		Hostname: "127.0.0.1",
		RaftPort: "7000",
		HttpPort: "8000",
	}
}
