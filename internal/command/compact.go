package command

type CompactCmd struct {
	TargetRev int64
}

type CompactResult struct {
	DoneC <-chan struct{}
	Err   error
}
