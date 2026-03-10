package compactor

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
	CompactorPeriod          CompactorKind = "periodic"
	CompactorWindowRetention CompactorKind = "window_retention"
)

type CompactorOptions struct {
	// kind of compactor to instantiate
	Kind CompactorKind `json:"compactor_kind"`
	// threshold for triggering compaction, to ensure compaction only runs
	// after we have at least this amount of revisions since last compaction
	Threshold int64 `json:"compactor_threshold"`
	// for periodic compactor, the period between compactions
	Period time.Duration `json:"compactor_period_ns"`
	// or window retention compactor, interval determines how often the compactor is triggered
	PollInterval time.Duration `json:"compactor_poll_interval_ns"`
	// for window retention compactor, the size of the retention window in number of revisions
	WindowSize int64 `json:"compactor_window_size"`
}

func (o CompactorOptions) Validate() error {
	switch o.Kind {
	case CompactorWindowRetention:
		if o.WindowSize <= 0 {
			return fmt.Errorf("compactor: window size must be positive for window retention")
		}
		if o.Threshold <= 0 {
			return fmt.Errorf("compactor: threshold must be positive for window retention")
		}
		if o.PollInterval <= 0 {
			return fmt.Errorf("compactor: poll interval must be positive for window retention")
		}
	case CompactorPeriod:
		if o.Period <= 0 {
			return fmt.Errorf("compactor: period must be positive for periodic compactor")
		}
	default:
		return fmt.Errorf("compactor: unrecognised compactor kind: %s", o.Kind)
	}
	return nil
}

func New(logger *slog.Logger, compactable Compactable, opts CompactorOptions) Compactor {
	switch opts.Kind {
	case CompactorWindowRetention:
		return newWindowRetentionCompactor(logger, compactable, opts)
	case CompactorPeriod:
		panic(fmt.Sprintf("%s not yet implemented", CompactorPeriod))
	default:
		panic(fmt.Sprintf("unrecognised compactor kind: %s", opts.Kind))
	}
}
