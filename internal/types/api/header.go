package api

type ResponseHeader struct {
	// Revision az írás végrehajtása utáni verziósorszámot tartalmazza.
	// Olvasás esetén ez a verziósorszám, amin a lekérdezést kiszolgáltuk
	Revision int64 `json:"revision"`
	// CompactedRev a legujtóbbi tömörített verziósorszám
	CompactedRev int64 `json:"compacted_revision"`
	// RaftTerm a jelenlegi raft term
	RaftTerm uint64 `json:"raft_term"`
	// RaftIndex a jelenlegi raft index
	RaftIndex uint64 `json:"raft_index"`
	// NodeID a végrahtó node azonosítója
	NodeID string `json:"node_id"`
}
