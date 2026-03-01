package compaction

import (
	"errors"
	"fmt"
	"math"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage"
)

var (
	ErrRevCompacted     = errors.New("compaction error: requested revision is older than compacted revision")
	ErrCompactionFailed = errors.New("compaction failed")
)

type Compactor interface {
	Compact(cutoff int64) error
	ComputeCutoff() int64
}

type compactor struct {
	store      storage.Storage
	revMgr     kv.RevisionManager
	windowSize uint64
}

func NewCompactor(store storage.Storage, revMgr kv.RevisionManager, windowSize uint64) Compactor {
	return &compactor{
		store:  store,
		revMgr: revMgr,
	}
}

// / TODODODOOD
func (c *compactor) ComputeCutoff() int64 {
	var min int64 = math.MaxInt64

	c.store.Scan(kv.BucketKeyHistory, func(key, value []byte) bool {
		ck, err := kv.DecodeCompositeKey(key)
		if err != nil {
			// skip corrupted keys, or if decode failed
			return true
		}
		if min > ck.Rev.Main {
			min = ck.Rev.Main
		}

		return true
	})

	return min
}

// Compact deletes all entires in the history log (key, rev) -> value
// where rev < C and C is the cutoff revision computed earlier
func (c *compactor) Compact(C int64) error {
	err := c.store.Compact(kv.BucketKeyHistory, func(key []byte) bool {
		ck, err := kv.DecodeCompositeKey(key)
		if err != nil {
			// for now keep corrupted keys (or decode error) in the db
			// so we can debug
			return false
		}
		return ck.Rev.Main < C
	})

	if err != nil {
		return fmt.Errorf("%w: %v", kv.ErrEncodingFailed, err)
	}

	return nil
}
