package util

import (
	"fmt"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/hashicorp/raft"
)

// ProposeFunc egy callback, amivel az fsm-nek tudunk benyújtani új parancsot
// anélkük, hogy egy *raft.Raft példányt tároljunk minden egyes structunkban
type ProposeFunc func(cmd command.Command) (raft.ApplyFuture, error)

const applyTimeout = 0 * time.Millisecond

func NewProposeFunc(r *raft.Raft) ProposeFunc {
	return func(cmd command.Command) (raft.ApplyFuture, error) {
		bytes, err := command.Encode(cmd)
		if err != nil {
			return nil, fmt.Errorf("proposing command failed: %w", err)
		}
		return r.Apply(bytes, applyTimeout), nil
	}
}
