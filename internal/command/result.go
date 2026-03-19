package command

// A Result a Command végrehajtásának eredményét tartalmazza, a Header pedig a művelethez tartozó reviziót
type Result struct {
	Header ResultHeader `json:"header"`
	Error  error        `json:"error,omitempty"`

	PutResult            *PutResult            `json:"put,omitempty"`
	DeleteResult         *DeleteResult         `json:"delete,omitempty"`
	TxnResult            *TxnResult            `json:"txn,omitempty"`
	RangeResult          *RangeResult          `json:"range,omitempty"`
	LeaseGrantResult     *LeaseGrantResult     `json:"lease_grant,omitempty"`
	LeaseRevokeResult    *LeaseRevokeResult    `json:"lease_revoke,omitempty"`
	LeaseKeepAliveResult *LeaseKeepAliveResult `json:"lease_keep_alive,omitempty"`

	LeaseExpireResult *LeaseExpireResult
	CompactResult     *CompactResult
}

// A ResultHeader minden eredmény közös metaadatait tartalmazza
type ResultHeader struct {
	// Revision az írás végrehajtása utáni revíziót tartalmazza. Olvasás esetén ez a revízió, amin a lekérdezést kiszolgáltuk
	Revision int64 `json:"revision"`
	// NodeID a végrahtó node azonosítója
	NodeID string `json:"node_id"`
	// RaftTerm a jelenlegi raft term
	RaftTerm uint64 `json:"raft_term"`
	// RaftIndex a jelenlegi raft index
	RaftIndex uint64 `json:"raft_index"`
}
