package command

// A Result a Command végrehajtásának eredményét tartalmazza, a Header pedig a művelethez tartozó reviziót
type Result struct {
	Header ResultHeader `json:"header"`
	Error  error        `json:"error,omitempty"`

	Put            *PutResult            `json:"put,omitempty"`
	Delete         *DeleteResult         `json:"delete,omitempty"`
	Txn            *TxnResult            `json:"txn,omitempty"`
	Range          *RangeResult          `json:"range,omitempty"`
	LeaseGrant     *LeaseGrantResult     `json:"lease_grant,omitempty"`
	LeaseRevoke    *LeaseRevokeResult    `json:"lease_revoke,omitempty"`
	LeaseKeepAlive *LeaseKeepAliveResult `json:"lease_keep_alive,omitempty"`
	LeaseLookup    *LeaseLookupResult    `json:"lease_lookup,omitempty"`

	LeaseExpire *LeaseExpireResult
	Compact     *CompactResult
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
