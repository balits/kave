package command

import "github.com/balits/kave/internal/types/api"


type LeaseGrantCmd = api.LeaseGrantRequest
type LeaseGrantResult = api.LeaseGrantResponse

type LeaseRevokeCmd = api.LeaseRevokeRequest
type LeaseRevokeResult = api.LeaseRevokeResponse

type LeaseKeepAliveCmd = api.LeaseKeepAliveRequest
type LeaseKeepAliveResult = api.LeaseKeepAliveResponse

type LeaseLookupCmd = api.LeaseLookupRequest
type LeaseLookupResult = api.LeaseLookupResponse

// LeaseCheckpointCmd is non-public command coming from lease.CheckpointScheduler
// and it refreshed the leases remaining TTLs periodically
type LeaseCheckpointCmd struct {
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	LeaseID      int64
	RemainingTTL int64
}

// LeaseExpireCmd is non-public command coming from lease.ExpiryLoop
// and it evicts expired leases and their attached keys from the db
type LeaseExpireCmd struct {
	ExpiredIDs []int64 // kitörlendő lease-ek ID-jai
}

type LeaseExpireResult struct {
	RemovedLeaseCount int
	RemovedKeyCount   int
}
