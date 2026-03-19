package command

import "github.com/balits/kave/internal/types/api"

// publikus parancsok, a kliens ezeket éri csak el

type LeaseGrantCmd = api.LeaseGrantRequest
type LeaseGrantResult = api.LeaseGrantResponse

type LeaseRevokeCmd = api.LeaseRevokeRequest
type LeaseRevokeResult = api.LeaseRevokeResponse

type LeaseKeepAliveCmd = api.LeaseKeepAliveRequest
type LeaseKeepAliveResult = api.LeaseKeepAliveResponse

type LeaseCheckpointCmd struct {
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	LeaseID      int64
	RemainingTTL int64
}

type LeaseExpireCmd struct {
	ExpiredIDs []int64 // kitörlendő lease-ek ID-jai
}

type LeaseExpireResult struct {
	RemovedLeaseCount int
	RemovedKeyCount   int
}
