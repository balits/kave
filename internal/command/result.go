package command

import "github.com/balits/kave/internal/types/api"

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
type ResultHeader = api.ResponseHeader
