package mvcc

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage/backend"
)

const COMPACTION_BATCH_LIMIT int = 100

type KVStore struct {
	rwlock           sync.RWMutex    // mutex for the whole store, not for backend transactions, used for raft meta and compaction
	backend          backend.Backend // storage backend
	kvIndex          kv.Index        // key chache
	revMu            sync.RWMutex    // revision mutex
	currentRev       kv.Revision     // up to date revision, updated by writers, read by readers, protected by revMu
	compactedMainRev int64           // main revision up to which the store has been compacted, protected by revMu
	raftTerm         uint64          // latest applied raft term, protected by rwlock
	raftIndex        uint64          // latest applied raft index, protected by rwlock
	logger           *slog.Logger
}

func NewKVStore(logger *slog.Logger, b backend.Backend) *KVStore {
	return &KVStore{
		backend: b,
		kvIndex: kv.NewTreeIndex(logger),
		logger:  logger.With("component", "kvstore"),
	}
}

func (s *KVStore) Revision() kv.Revision {
	s.revMu.RLock()
	defer s.revMu.RUnlock()
	return s.currentRev
}

// Writer acquires RLock so multiple writers can proceed concurrently
// (they're serialized by raft anyway, so only one writer exists at a time,
// but the RLock is correct because writers don't mutate KVStore's structure)
//
// Locks get releaseed in Writer.End()
//
// Locking the store rwlock blocks concurrent writes and restores
func (s *KVStore) NewWriter() Writer {
	// locks gets released in writer.End()
	s.rwlock.RLock()
	w := &writer{
		store:    s,
		writeTx:  s.backend.WriteTx(),
		startRev: s.currentRev,
	}
	w.writeTx.Lock()
	return w
}

func (s *KVStore) NewReader() Reader {
	return &reader{store: s}
}

func (s *KVStore) UpdateRaftMeta(logIndex, term uint64) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	s.raftIndex = logIndex
	s.raftTerm = term
}

func (s *KVStore) RaftMeta() (logIndex, term uint64) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.raftIndex, s.raftTerm
}

func (s *KVStore) Restore(r io.Reader) error {
	if err := s.backend.Close(); err != nil {
		s.logger.Error("restore error: failed to close backend", "error", err)
	}
	if err := s.backend.Restore(r); err != nil {
		s.logger.Error("restore error: failed to restore backend", "error", err)
		return err
	}

	lastRev := kv.Revision{Main: 0, Sub: 0}
	min := kv.RevToBytes(lastRev, kv.NewRevBytes())
	max := kv.RevToBytes(kv.Revision{Main: math.MaxInt64, Sub: math.MaxInt64}, kv.NewRevBytes())

	s.kvIndex.Clear()
	rtx := s.backend.ReadTx()
	rtx.RLock()
	rtx.UnsafeScan(kv.BucketMain, min, max, func(k, v []byte) error {
		bk := kv.BytesToBucketKey(k)
		entry, err := kv.DecodeEntry(v)
		if err != nil {
			s.logger.Error("restore error: failed to decode entry", "error", err)
		}
		if err := s.kvIndex.Put(entry.Key, bk.Revision); err != nil {
			s.logger.Error("restore error: failed to put entry into kvIndex", "error", err)
		}
		lastRev = bk.Revision
		return nil
	})

	raftTermBytes, err := rtx.UnsafeGet(kv.BucketMeta, kv.MetaKeyRaftTerm)
	if err != nil {
		s.logger.Error("restore error: failed to get raft term", "error", err)
	}
	raftIndexBytes, err := rtx.UnsafeGet(kv.BucketMeta, kv.MetaKeyRaftApplyIndex)
	if err != nil {
		s.logger.Error("restore error: failed to get raft index", "error", err)
	}
	rtx.RUnlock()
	term, err := kv.DecodeUint64(raftTermBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode raft term", "error", err)
	}
	raftIndex, err := kv.DecodeUint64(raftIndexBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode raft index", "error", err)
	}

	s.rwlock.Lock()
	s.raftTerm = term
	s.raftIndex = raftIndex
	s.rwlock.Unlock()

	s.revMu.Lock()
	s.currentRev = lastRev
	s.revMu.Unlock()

	return nil
}

func (s *KVStore) Snapshot() Snapshot {
	return Snapshot{s}
}

// Compact compacts the store up to the given revision.
// All superseded key revisions with main revision < rev will be removed.
// Compaction happens in two phases, the last happends concurrently
// 1) Persist schedule compaction revision -> crash safe
// 2) Execute comapction + Update finished compaction revision
func (s *KVStore) Compact(rev int64) (<-chan struct{}, error) {
	s.revMu.Lock()
	if rev <= 0 {
		return nil, fmt.Errorf("compaction error: compaction rev must be > 0")
	} else if rev > s.currentRev.Main {
		return nil, fmt.Errorf("compaction error: compaction rev must be <= current revision")
	} else if rev <= s.compactedMainRev {
		return nil, kv.ErrCompacted
	}
	s.revMu.Unlock()
	// Persist schedule compaction revision -> crash safe
	{
		wtx := s.backend.WriteTx()
		wtx.Lock()
		revBytes := kv.RevToBytes(kv.Revision{Main: rev}, kv.NewRevBytes())
		wtx.UnsafePut(kv.BucketMeta, kv.MetaKeyCompactScheduled, revBytes)
		wtx.Commit()
		wtx.Unlock()
	}

	// Execute comapction
	c := make(chan struct{})
	go func() {
		defer close(c)
		// Update finished schedulde compaction revision inside doCompact
		s.doCompact(rev)
	}()

	return c, nil
}

func (s *KVStore) doCompact(rev int64) {
	s.logger.Info("compaction started", "revision", rev)
	// 1) collect values we still should retain
	retain, err := s.kvIndex.Compact(rev)
	if err != nil {
		s.logger.Error("compaction error: failed to compact key index", "error", err, "revision", rev)
		return
	}

	// 2) delete en-masse entries where entry.modRev <= rev
	start := kv.RevToBytes(kv.Revision{}, kv.NewRevBytes())
	endExcluded := kv.RevToBytes(kv.Revision{Main: rev + 1}, kv.NewRevBytes())

	// batch deletes
	var (
		done           bool
		numDeleted     int
		lastRevVisited kv.Revision
		maxBatchSize   = COMPACTION_BATCH_LIMIT
	)
	for {
		wtx := s.backend.WriteTx()
		wtx.Lock()
		var currBatch = 0

		err := wtx.UnsafeScan(kv.BucketMain, start, endExcluded, func(k, v []byte) error {
			if currBatch >= maxBatchSize {
				return fmt.Errorf("maxBatchSize exceeded")
			}
			currBatch++

			bk := kv.BytesToBucketKey(k)
			lastRevVisited = bk.Revision

			if _, shouldRetain := retain[bk.Revision]; !shouldRetain {
				if err := wtx.UnsafeDelete(kv.BucketMain, k); err != nil {
					return err
				}
				numDeleted++
			}
			return nil
		})

		if err != nil {
			s.logger.Error("compaction error: scanning returned an error", "error", err)
		}

		if err == nil || currBatch < maxBatchSize {
			// were finished
			done = true
		}

		if done {
			// persist meta
			revBytes := kv.RevToBytes(kv.Revision{Main: rev}, kv.NewRevBytes())
			wtx.UnsafePut(kv.BucketMeta, kv.MetaKeyCompactFinished, revBytes)
		}

		if err := wtx.Commit(); err != nil {
			s.logger.Error("compaction error: commit failed", "error", err)
			wtx.Unlock()
			return
		}
		wtx.Unlock()

		if done {
			break
		}

		start = kv.RevToBytes(lastRevVisited.AddSub(1), kv.NewRevBytes())
	}

	s.revMu.Lock()
	s.compactedMainRev = rev
	s.revMu.Unlock()

	s.logger.Info("finished compaction", "revision", rev, "num_deleted_keys", numDeleted)
}
