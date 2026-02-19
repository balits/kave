package fsm

// ApplyResult is the type returned by FSM.Apply overwriting the any typed return value
// in hc's raft, and is accessible by applyFuture.Result().
// For batch operations, the BatchResult gives back the stored key value pairs.
type AppyResult struct {
	err          error

	// only one of these will be non null at once
	GetResult    *GetResult
	SetResult    *SetResult
	DeleteResult *DeleteResult
	BatchResult  *BatchResult
	CASResult    *SetResult
}

func (ar AppyResult) Error() error {
	return ar.err
}

type SetResult = Entry

type GetResult = Entry

type DeleteResult struct {
	Deleted   bool   `json:"deleted"`
	PrevEntry *Entry `json:"prev_kv,omitempty"`
}

type BatchResult struct {
	Success bool `json:"success"`
}
