package api

// A ResponseHeader minden eredmény közös metaadatait tartalmazza
type ResponseHeader struct {
	// Revision az írás végrehajtása utáni revíziót tartalmazza. Olvasás esetén ez a revízió, amin a lekérdezést kiszolgáltuk
	Revision int64 `json:"revision"`
	// NodeID a végrahtó node azonosítója
	NodeID string `json:"node_id"`
	// RaftTerm a jelenlegi raft term
	RaftTerm uint64 `json:"raft_term"`
	// RaftIndex a jelenlegi raft index
	RaftIndex uint64 `json:"raft_index"`
}
