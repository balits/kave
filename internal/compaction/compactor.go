package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/mvcc"
)

type Compactable interface {
	mvcc.SmartRevisionGetter
	Compact(rev int64) (doneC <-chan struct{}, err error)
}

type Compactor interface {
	Run(context.Context)
	Pause()
	Resume()
	Stop()
}

// TODO: fix logger string() error on kind
type CompactorKind string

const (
	CompactorPeriod          CompactorKind = "periodic_compactor"
	CompactorWindowRetention CompactorKind = "window_retention_compactor"
)

type CompactorOptions struct {
	// kind of compactor to instantiate
	Kind CompactorKind
	// threshold for triggering compaction, to ensure compaction only runs
	// after we have at least this amount of revisions since last compaction
	Threshold int64

	// or window retention compactor, interval determines how often the compactor is triggered
	PollInterval time.Duration
	// for window retention compactor, the size of the retention window in number of revisions
	WindowSize int64

	// for periodic compactor, the period between compactions
	Period time.Duration
}

func NewCompactor(logger *slog.Logger, compactable Compactable, opts CompactorOptions) Compactor {
	if opts.Kind == CompactorPeriod {
		panic(fmt.Sprintf("%s not yet implemented", CompactorPeriod))
	}

	return newWindowRetentionCompactor(logger, compactable, opts)
}
