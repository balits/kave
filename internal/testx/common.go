package testx

import (
	"fmt"
	"log/slog"
	"testing"
	"time"
)

const (
	WaitStateTimeout       = time.Millisecond * 400
	WaitFutureTimeout      = time.Millisecond * 1000
	WaitConsistencyTimeout = time.Millisecond * 2000
	WaitJoinTimeout        = time.Millisecond * 2000
)

type ErrTimeout struct {
	Timeout time.Duration
	Op      string
}

func (e ErrTimeout) Error() string {
	return fmt.Sprintf("waiting for %s timed out after %v", e.Op, e.Timeout)
}

func NewErrTimeout(t time.Duration, op string) ErrTimeout {
	return ErrTimeout{
		Timeout: t,
		Op:      op,
	}
}

func LogLevel() slog.Level {
	if testing.Verbose() {
		return slog.LevelDebug
	} else {
		var no slog.Level = 99
		return no
	}
}
