package common

// Metadata a jelenlegi állapotról, amelyet a HTTP válaszokban visszaadunk.
type Meta struct {
	ApplyIndex uint64 `json:"apply_index"`
	LastIndex  uint64 `json:"last_index"`
	Term       uint64 `json:"raft_term"`
	LeaderID   string `json:"leader_id"`
}
