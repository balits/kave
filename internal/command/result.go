package command

import "github.com/balits/kave/internal/types/api"

// A Result a Command végrehajtásának eredményét tartalmazza, a Header pedig a művelethez tartozó reviziót
type Result struct {
	Header ResultHeader `json:"header"`
	Error  error        `json:"error,omitempty"`

	// =====  public, client facing results =====

	Put            *ResultPut            `json:"put,omitempty"`
	Delete         *ResultDelete         `json:"delete,omitempty"`
	Txn            *ResultTxn            `json:"txn,omitempty"`
	Range          *ResultRange          `json:"range,omitempty"`
	LeaseGrant     *ResultLeaseGrant     `json:"lease_grant,omitempty"`
	LeaseRevoke    *ResultLeaseRevoke    `json:"lease_revoke,omitempty"`
	LeaseKeepAlive *ResultLeaseKeepAlive `json:"lease_keep_alive,omitempty"`
	LeaseLookup    *ResultLeaseLookup    `json:"lease_lookup,omitempty"`

	// =====  private, internal results =====

	LeaseExpire *ResultLeaseExpire
	Compaction  *CompactionResult
	OtWriteAll  *ResultOTWriteAll
}

// A ResultHeader minden eredmény közös metaadatait tartalmazza
type ResultHeader = api.ResponseHeader
