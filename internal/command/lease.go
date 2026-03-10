package command

type LeaseGrantCmd struct{}
type LeaseRevokeCmd struct{}

type LeaseCheckpointCmd struct {
	Checkpoints []Checkpoint
}

type Checkpoint struct {
	ID           int64
	RemainingTTL int64
}
