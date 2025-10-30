package util

import (
	"io"
	"log/slog"
)

//TODO: batch or disable logs in prod

func NewJSONLogger(logLevel string, out io.Writer) *slog.Logger {
	var level slog.Level
	switch logLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	return slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{
		Level: level,
		// AddSource: true,
	}))
}

type NullWriter struct{}

func (w NullWriter) Write(p []byte) (int, error) { return 0, nil }

func NewDiscardlLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(NullWriter{}, &slog.HandlerOptions{
		Level: slog.LevelError,
		// AddSource: true,
	}))
}
