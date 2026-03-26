package command

type CompactCmd struct {
	TargetRev int64
}

type ResultCompact struct {
	DoneC <-chan struct{}
	Error error
}
