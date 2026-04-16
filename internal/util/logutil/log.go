package logutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
)

//TODO: batch or disable logs in prod???

type Options struct {
	Kind  LoggerKind  `json:"kind"`
	Level LoggerLevel `json:"level"`
}

func (o *Options) Check() error {
	switch o.Kind {
	case LoggerKindJson, LoggerKindText, LoggerKindDiscard:
	default:
		return fmt.Errorf("invalid logger kind: '%s' (exptected %s | %s | %s) ", o.Kind, LoggerKindText, LoggerKindJson, LoggerKindDiscard)
	}

	switch o.Level {
	case LevelDebug, LevelInfo, LevelWarn, LevelError:
	default:
		return fmt.Errorf("invalid log level: '%s' (exptected %s | %s | %s | %s) ", o.Level, LevelDebug, LevelInfo, LevelWarn, LevelError)
	}

	return nil
}

func (l LoggerLevel) ToSlogLevel() (out slog.Level) {
	switch l {
	case LevelDebug:
		out = slog.LevelDebug
	case LevelInfo:
		out = slog.LevelInfo
	case LevelWarn:
		out = slog.LevelWarn
	case LevelError:
		out = slog.LevelError
	}
	return // 0 -> info
}

func ToLogutilLevel(l slog.Level) (out LoggerLevel) {
	switch l {
	case slog.LevelDebug:
		out = LevelDebug
	case slog.LevelInfo:
		out = LevelInfo
	case slog.LevelWarn:
		out = LevelWarn
	case slog.LevelError:
		out = LevelError
	default:
		out = LevelInfo
	}
	return
}

type LoggerKind string

const (
	LoggerKindJson    LoggerKind = "json"
	LoggerKindText    LoggerKind = "text"
	LoggerKindDiscard LoggerKind = "discard"
)

type LoggerLevel string

const (
	LevelDebug LoggerLevel = "debug"
	LevelInfo  LoggerLevel = "info"
	LevelWarn  LoggerLevel = "warn"
	LevelError LoggerLevel = "error"
)

func NewLogger(out io.Writer, opts Options) *slog.Logger {
	var handler slog.Handler
	handlerOpts := &slog.HandlerOptions{
		Level: opts.Level.ToSlogLevel(),
	}

	switch opts.Kind {
	case LoggerKindJson:
		handler = slog.NewJSONHandler(out, handlerOpts)
	case LoggerKindText:
		handler = slog.NewTextHandler(out, handlerOpts)
	case LoggerKindDiscard:
		//TODO: instead of Writer discarding things, create a handler that discards things
		handler = slog.NewTextHandler(NullWriter{}, handlerOpts)
	}
	return slog.New(handler)
}

func NewTestLogger(tb testing.TB, l slog.Level) *slog.Logger {
	return NewLogger(os.Stdout, Options{Kind: LoggerKindText, Level: ToLogutilLevel(l)})
}

type NullWriter struct{}

func (w NullWriter) Write(p []byte) (int, error) { return 0, nil }

func NewDiscardlLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(NullWriter{}, &slog.HandlerOptions{
		Level: slog.LevelError,
		// AddSource: true,
	}))
}

const SlogLevelTrace slog.Level = slog.LevelDebug - 1

// HcLogAdapter is an adapter that hold a *slog.Logger but also implements
// HashiCorps hclog.Logger. This way we can use slog.Logger not just in the application
// but also plug it into HashiCorps raft.Raft node
type HcLogAdapter struct {
	slogger     *slog.Logger
	level       hclog.Level
	namePrefix  string
	impliedArgs []interface{}
}

func hcLeveltoSlogLevel(lvl hclog.Level) slog.Level {
	switch lvl {
	case hclog.Trace:
		return SlogLevelTrace
	case hclog.Debug:
		return slog.LevelDebug
	case hclog.Info:
		return slog.LevelInfo
	case hclog.Warn:
		return slog.LevelWarn
	case hclog.Error:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

func slogLeveltoHcLevel(lvl slog.Level) hclog.Level {
	switch lvl {
	case SlogLevelTrace:
		return hclog.Trace
	case slog.LevelDebug:
		return hclog.Debug
	case slog.LevelInfo:
		return hclog.Info
	case slog.LevelWarn:
		return hclog.Warn
	case slog.LevelError:
		return hclog.Error
	default:
		return hclog.Info
	}
}

// ============ hclog.Logger impl ============

func (a *HcLogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	a.slogger.Log(context.Background(), hcLeveltoSlogLevel(level), a.namePrefix+msg, args...)
}

func (a *HcLogAdapter) Trace(msg string, args ...interface{}) {
	a.slogger.Log(context.Background(), SlogLevelTrace, a.namePrefix+msg, args...)
}

// Emit a message and key/value pairs at the DEBUG level
func (a *HcLogAdapter) Debug(msg string, args ...interface{}) {
	a.slogger.Debug(a.namePrefix+msg, args...)
}

// Emit a message and key/value pairs at the INFO level
func (a *HcLogAdapter) Info(msg string, args ...interface{}) {
	a.slogger.Info(a.namePrefix+msg, args...)
}

// Emit a message and key/value pairs at the WARN level
func (a *HcLogAdapter) Warn(msg string, args ...interface{}) {
	a.slogger.Warn(a.namePrefix+msg, args...)
}

// Emit a message and key/value pairs at the ERROR level
func (a *HcLogAdapter) Error(msg string, args ...interface{}) {
	a.slogger.Error(a.namePrefix+msg, args...)
}

// Indicate if TRACE logs would be emitted. This and the other Is* guards
// are used to elide expensive logging code based on the current level.
func (a *HcLogAdapter) IsTrace() bool {
	return a.level == hclog.Trace
}

// Indicate if DEBUG logs would be emitted. This and the other Is* guards
func (a *HcLogAdapter) IsDebug() bool {
	return a.level == hclog.Debug
}

// Indicate if INFO logs would be emitted. This and the other Is* guards
func (a *HcLogAdapter) IsInfo() bool {
	return a.level == hclog.Info
}

// Indicate if WARN logs would be emitted. This and the other Is* guards
func (a *HcLogAdapter) IsWarn() bool {
	return a.level == hclog.Warn
}

// Indicate if ERROR logs would be emitted. This and the other Is* guards
func (a *HcLogAdapter) IsError() bool {
	return a.level == hclog.Error
}

// ImpliedArgs returns With key/value pairs
func (a *HcLogAdapter) ImpliedArgs() []interface{} {
	return a.impliedArgs
}

// Creates a sublogger that will always have the given key/value pairs
func (a *HcLogAdapter) With(args ...interface{}) hclog.Logger {
	return &HcLogAdapter{
		slogger: a.slogger.With(args...),
	}
}

func (a *HcLogAdapter) Name() string {
	if a.namePrefix == "" {
		return ""
	}
	return a.namePrefix[:len(a.namePrefix)-2]
}

// Create a logger that will prepend the name string on the front of all messages.
// If the logger already has a name, the new value will be appended to the current
// name. That way, a major subsystem can use this to decorate all it's own logs
// without losing context.
func (a *HcLogAdapter) Named(name string) hclog.Logger {
	slogger := cloneSlogger(a.slogger)

	return &HcLogAdapter{
		slogger:     slogger,
		namePrefix:  a.namePrefix + name + ": ",
		level:       a.level,
		impliedArgs: a.impliedArgs,
	}
}

// Create a logger that will prepend the name string on the front of all messages.
// This sets the name of the logger to the value directly, unlike Named which honor
// the current name as well.
func (a *HcLogAdapter) ResetNamed(name string) hclog.Logger {
	var prefix string
	if name == "" {
		prefix = ""
	} else {
		prefix = name + ": "
	}

	cloned := cloneSlogger(a.slogger)
	return &HcLogAdapter{
		slogger:     cloned,
		level:       a.level,
		namePrefix:  prefix,
		impliedArgs: a.impliedArgs,
	}

}

// Updates the level. This should affect all related loggers as well,
// unless they were created with IndependentLevels. If an
// implementation cannot update the level on the fly, it should no-op.
func (a *HcLogAdapter) SetLevel(level hclog.Level) {
	// no-op
}

// Returns the current level
func (a *HcLogAdapter) GetLevel() hclog.Level {
	return a.level
}

// Return a value that conforms to the stdlib log.Logger interface
func (a *HcLogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	if opts == nil {
		opts = &hclog.StandardLoggerOptions{}
	}

	return log.New(a.StandardWriter(opts), "", 0)
}

// Return a value that conforms to io.Writer, which can be passed into log.SetOutput()
func (a *HcLogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	a2 := cloneHcLogAdapter(a)

	return &stdlogAdapter{
		log:                      a2,
		inferLevels:              opts.InferLevels,
		inferLevelsWithTimestamp: opts.InferLevelsWithTimestamp,
		forceLevel:               opts.ForceLevel,
	}
}

func cloneSlogger(l *slog.Logger) *slog.Logger {
	c := *l
	return &c
}

func cloneHcLogAdapter(a *HcLogAdapter) *HcLogAdapter {
	slogger := cloneSlogger(a.slogger)
	return &HcLogAdapter{
		slogger:     slogger,
		namePrefix:  a.namePrefix,
		level:       a.level,
		impliedArgs: a.impliedArgs,
	}
}

func NewHcLogAdapter(slogger *slog.Logger, level slog.Level) *HcLogAdapter {
	return &HcLogAdapter{
		slogger: slogger,
		level:   slogLeveltoHcLevel(level),
	}
}

// This was taken from go-hclog src
// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MIT

// beginning of inputs.
var logTimestampRegexp = regexp.MustCompile(`^[\d\s\:\/\.\+-TZ]*`)

// Provides a io.Writer to shim the data out of *log.Logger
// and back into our Logger. This is basically the only way to
// build upon *log.Logger.
type stdlogAdapter struct {
	log                      hclog.Logger
	inferLevels              bool
	inferLevelsWithTimestamp bool
	forceLevel               hclog.Level
}

// Take the data, infer the levels if configured, and send it through
// a regular Logger.
func (s *stdlogAdapter) Write(data []byte) (int, error) {
	str := string(bytes.TrimRight(data, " \t\n"))

	if s.forceLevel != hclog.NoLevel {
		// Use pickLevel to strip log levels included in the line since we are
		// forcing the level
		_, str := s.pickLevel(str)

		// Log at the forced level
		s.dispatch(str, s.forceLevel)
	} else if s.inferLevels {
		if s.inferLevelsWithTimestamp {
			str = s.trimTimestamp(str)
		}

		level, str := s.pickLevel(str)
		s.dispatch(str, level)
	} else {
		s.log.Info(str)
	}

	return len(data), nil
}

func (s *stdlogAdapter) dispatch(str string, level hclog.Level) {
	switch level {
	case hclog.Trace:
		s.log.Trace(str)
	case hclog.Debug:
		s.log.Debug(str)
	case hclog.Info:
		s.log.Info(str)
	case hclog.Warn:
		s.log.Warn(str)
	case hclog.Error:
		s.log.Error(str)
	default:
		s.log.Info(str)
	}
}

// Detect, based on conventions, what log level this is.
func (s *stdlogAdapter) pickLevel(str string) (hclog.Level, string) {
	switch {
	case strings.HasPrefix(str, "[DEBUG]"):
		return hclog.Debug, strings.TrimSpace(str[7:])
	case strings.HasPrefix(str, "[DEBUG-4]"):
		return hclog.Trace, strings.TrimSpace(str[9:])
	case strings.HasPrefix(str, "[INFO]"):
		return hclog.Info, strings.TrimSpace(str[6:])
	case strings.HasPrefix(str, "[WARN]"):
		return hclog.Warn, strings.TrimSpace(str[6:])
	case strings.HasPrefix(str, "[ERROR]"):
		return hclog.Error, strings.TrimSpace(str[7:])
	case strings.HasPrefix(str, "[ERR]"):
		return hclog.Error, strings.TrimSpace(str[5:])
	default:
		return hclog.Info, str
	}
}

func (s *stdlogAdapter) trimTimestamp(str string) string {
	idx := logTimestampRegexp.FindStringIndex(str)
	return str[idx[1]:]
}
