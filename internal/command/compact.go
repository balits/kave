package command

import "github.com/balits/kave/internal/types/api"

type CompactionCmd = api.CompactionRequest

type CompactionResult struct {
	DoneC <-chan struct{}
	Error error
}
