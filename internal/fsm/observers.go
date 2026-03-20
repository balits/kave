package fsm

type LeadershipObserver interface {
	OnLeadershipGranted()
	OnLeadershipLost()
}

type WriteObserver interface {
	OnWrite(currentRev int64)
}
