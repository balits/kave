package mvcc

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types"
	"github.com/prometheus/client_golang/prometheus"
)

// SmartRevisionGetter visszadja a jelenlegi, illetve a kompaktált reviziót
// így csak egyszer kell lockolni a store-t
type SmartRevisionGetter interface {
	Revisions() (currentRev kv.Revision, compacted int64)
}

const COMPACTION_BATCH_LIMIT int = 1024

type KVStore struct {
	rwlock           sync.RWMutex    // mutex for the whole store, not for backend transactions, used for raft meta and compaction
	backend          backend.Backend // storage backend
	kvIndex          kv.Index        // key chache
	revMu            sync.RWMutex    // revision mutex
	currentRev       kv.Revision     // up to date revision, updated by writers, read by readers, protected by revMu
	compactedMainRev int64           // main revision up to which the store has been compacted, protected by revMu
	raftTerm         uint64          // latest applied raft term, protected by rwlock
	applyIndex       uint64          // latest applied raft index, protected by rwlock
	logger           *slog.Logger

	metrics *metrics.KVMetrics
}

func NewKVStore(reg prometheus.Registerer, logger *slog.Logger, b backend.Backend) *KVStore {
	s := &KVStore{
		backend: b,
		kvIndex: kv.NewTreeIndex(logger),
		logger:  logger.With("component", "kvstore"),
	}
	s.metrics = newKVMetrics(reg, s)

	return s
}

func (s *KVStore) Revisions() (currentRev kv.Revision, compacted int64) {
	s.revMu.RLock()
	defer s.revMu.RUnlock()
	return s.currentRev, s.compactedMainRev
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
	w := newWriter(s, s.backend.WriteTx(), s.currentRev)
	w.writeTx.Lock()
	return w
}

func (s *KVStore) NewReader() Reader {
	return &reader{store: s, metrics: s.metrics}
}

func (s *KVStore) UpdateRaftMeta(logIndex, term uint64) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	s.applyIndex = logIndex
	s.raftTerm = term
}

func (s *KVStore) RaftMeta() (logIndex, term uint64) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.applyIndex, s.raftTerm
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
	min := kv.EncodeRevision(lastRev, kv.NewRevBytes())
	max := kv.EncodeRevision(kv.Revision{Main: math.MaxInt64, Sub: math.MaxInt64}, kv.NewRevBytes())

	s.kvIndex.Clear()
	rtx := s.backend.ReadTx()
	rtx.RLock()
	rtx.UnsafeScan(schema.BucketKV, min, max, func(k, v []byte) error {
		bk := kv.DecodeKVBucketKey(k)
		entry, err := types.DecodeEntry(v)
		if err != nil {
			s.logger.Error("restore error: failed to decode entry", "error", err)
		}
		if err := s.kvIndex.Put(entry.Key, bk.Revision); err != nil {
			s.logger.Error("restore error: failed to put entry into kvIndex", "error", err)
		}
		lastRev = bk.Revision
		return nil
	})

	raftTermBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.MetaKeyRaftTerm)
	if err != nil {
		s.logger.Error("restore error: failed to get raft term", "error", err)
	}
	raftIndexBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.MetaKeyRaftApplyIndex)
	if err != nil {
		s.logger.Error("restore error: failed to get raft index", "error", err)
	}
	finishedCompactedRevBytes, err := rtx.UnsafeGet(schema.BucketMeta, schema.MetaKeyFinishedCompactionRev)
	if err != nil {
		s.logger.Error("restore error: failed to finished compacted rev", "error", err)
	}

	rtx.RUnlock()
	term, err := types.DecodeUint64(raftTermBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode raft term", "error", err)
	}
	raftIndex, err := types.DecodeUint64(raftIndexBytes)
	if err != nil {
		s.logger.Error("restore error: failed to decode raft index", "error", err)
	}

	finished := kv.DecodeRevision(finishedCompactedRevBytes)

	s.rwlock.Lock()
	s.raftTerm = term
	s.applyIndex = raftIndex
	s.rwlock.Unlock()

	s.revMu.Lock()
	s.currentRev = lastRev
	s.compactedMainRev = finished.Main
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
	defer s.revMu.Unlock() // lock rev for the whole compaction, so that no other gorutine could schedule a compaction
	if rev <= 0 {
		return nil, fmt.Errorf("compaction error: compaction target revision cannot be negative")
	} else if rev > s.currentRev.Main {
		return nil, fmt.Errorf("compaction error: compaction target revision cannot be higher than current revision")
	} else if rev <= s.compactedMainRev {
		return nil, fmt.Errorf("compaction error: %v", kv.ErrCompacted)
	}

	// Persist scheduled compaction revision -> crash safe
	{
		wtx := s.backend.WriteTx()
		wtx.Lock()
		revBytes := kv.EncodeRevision(kv.Revision{Main: rev}, kv.NewRevBytes())
		wtx.UnsafePut(schema.BucketMeta, schema.MetaKeyScheduledCompactionRev, revBytes)
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

	// Persist finished compaction revision -> on restore if scheduled != finished we havent compacted until target rev
	{
		wtx := s.backend.WriteTx()
		wtx.Lock()
		revBytes := kv.EncodeRevision(kv.Revision{Main: rev}, kv.NewRevBytes())
		wtx.UnsafePut(schema.BucketMeta, schema.MetaKeyFinishedCompactionRev, revBytes)
		wtx.Commit()
		wtx.Unlock()
	}

	// TODO: return real errors from .doCompact()
	return c, nil
}

func (s *KVStore) doCompact(rev int64) {
	s.logger.Info("compacting kvstore",
		"target_rev", rev,
		"prev_compacted_rev", s.compactedMainRev,
		"current_rev", s.currentRev.Main,
	)
	// collect values we still should retain
	retain, err := s.kvIndex.Compact(rev)
	if err != nil {
		s.logger.Error("compaction error: failed to compact key index", "error", err, "revision", rev)
		return
	}

	start := kv.EncodeRevision(kv.Revision{Main: s.compactedMainRev}, kv.NewRevBytes())
	endExcluded := kv.EncodeRevision(kv.Revision{Main: rev + 1}, kv.NewRevBytes())

	var (
		done           bool
		numDeleted     int
		lastRevVisited kv.Revision
		maxBatchSize   = COMPACTION_BATCH_LIMIT
		batch          = make([][]byte, 0, maxBatchSize)
	)
	for {
		wtx := s.backend.WriteTx()
		wtx.Lock()
		clear(batch)

		err := wtx.UnsafeScan(schema.BucketKV, start, endExcluded, func(k, v []byte) error {
			if len(batch) == maxBatchSize {
				return fmt.Errorf("maxBatchSize exceeded")
			}
			bk := kv.DecodeKVBucketKey(k)
			lastRevVisited = bk.Revision

			if _, shouldRetain := retain[bk.Revision]; !shouldRetain {
				batch = append(batch, append([]byte{}, k...))
			}
			return nil
		})

		if err != nil {
			s.logger.Warn("compaction error: scanning returned an error", "error", err)
		}

		for _, k := range batch {
			err := wtx.UnsafeDelete(schema.BucketKV, k)
			if err != nil && !errors.Is(err, storage.ErrEmptyKey) {
				s.logger.Warn("compaction error: failed to delete key", "error", err, "key", fmt.Sprintf("%b", k))
			} else {
				numDeleted++
			}
		}

		if err == nil || len(batch) < maxBatchSize {
			// were finished
			done = true
		}

		if done {
			// persist meta
			revBytes := kv.EncodeRevision(kv.Revision{Main: rev}, kv.NewRevBytes())
			wtx.UnsafePut(schema.BucketMeta, schema.MetaKeyFinishedCompactionRev, revBytes)
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

		start = kv.EncodeRevision(lastRevVisited.AddSub(1), kv.NewRevBytes())
	}

	s.revMu.Lock()
	s.compactedMainRev = rev
	s.revMu.Unlock()

	s.logger.Info("finished compaction", "revision", rev, "num_deleted_keys", numDeleted)
}

func (s *KVStore) Ping() error {
	return s.backend.Ping()
}

// NOTE: this is just a hack to use in tests
// it just closes the backend
// Doesnt clear the index or anything
func (s *KVStore) Close() error {
	return s.backend.Close()
}
