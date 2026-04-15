package api

type CompactionRequest struct {
	TargetRev int64 `json:"target_rev"`
}

type CompactionResponseNoHeader struct {
	Success bool `json:"success"`
}

type CompactionResponse struct {
	CompactionResponseNoHeader
	Header ResponseHeader `json:"header"`
}
