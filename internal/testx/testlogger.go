package testx

import (
	"io"
	"log/slog"
	"testing"

	"github.com/neilotoole/slogt"
)

func GetTestingLogLevel() slog.Level {
	if testing.Verbose() {
		return slog.LevelInfo
	} else {
		var no slog.Level = 99
		return no
	}
}

func NewTestLogger(tb testing.TB, logLevel slog.Level) *slog.Logger {
	loggerFactory := slogt.Factory(func(w io.Writer) slog.Handler {
		opts := &slog.HandlerOptions{
			Level: logLevel,
		}
		return slog.NewTextHandler(w, opts)
	})
	return slogt.New(tb, loggerFactory)
}
