package command

type LeaseGrantCmd struct{}
type LeaseRevokeCmd struct {
	LeaseID int64
}

type LeaseCheckpointCmd struct {
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	ID           int64
	RemainingTTL int64
}
