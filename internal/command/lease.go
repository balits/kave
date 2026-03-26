package command

import "github.com/balits/kave/internal/types/api"

type CmdLeaseGrant = api.LeaseGrantRequest
type ResultLeaseGrant = api.LeaseGrantResponse

type CmdLeaseRevoke = api.LeaseRevokeRequest
type ResultLeaseRevoke = api.LeaseRevokeResponse

type CmdLeaseKeepAlive = api.LeaseKeepAliveRequest
type ResultLeaseKeepAlive = api.LeaseKeepAliveResponse

type CmdLeaseLookup = api.LeaseLookupRequest
type ResultLeaseLookup = api.LeaseLookupResponse

// CmdLeaseCheckpoint is non-public command coming from lease.CheckpointScheduler
// and it refreshed the leases remaining TTLs periodically
type CmdLeaseCheckpoint struct {
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	LeaseID      int64
	RemainingTTL int64
}

// CmdLeaseExpire is non-public command coming from lease.ExpiryLoop
// and it evicts expired leases and their attached keys from the db
type CmdLeaseExpire struct {
	ExpiredIDs []int64 // kitörlendő lease-ek ID-jai
}

type ResultLeaseExpire struct {
	RemovedLeaseCount int
	RemovedKeyCount   int
}
