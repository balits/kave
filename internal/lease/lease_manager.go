package lease

import (
	"container/heap"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	errLease           = errors.New("lease error:")
	errLeaseIDConflict = fmt.Errorf("%w: lease ID conflict", errLease)
	errLeaseNotFound   = fmt.Errorf("%w: lease not found", errLease)

	errTTLNegative = fmt.Errorf("%w: TTL cannot be negative or zero", errLease)
)

type LeaseManager struct {
	rwlock   sync.RWMutex
	leaseMap map[int64]*Lease
	heap     heap.Interface
	heapMap  map[int64]*heapItem
	store    *mvcc.KVStore
	backend  backend.Backend
	metrics  *metrics.LeaseMetrics
	logger   *slog.Logger
}

func NewLeaseManager(reg prometheus.Registerer, logger *slog.Logger, store *mvcc.KVStore, backend backend.Backend) *LeaseManager {
	var h leaseHeap = make(leaseHeap, 0, 32)
	heap.Init(&h)
	m := &LeaseManager{
		leaseMap: make(map[int64]*Lease),
		heap:     &h,
		heapMap:  make(map[int64]*heapItem),
		store:    store,
		backend:  backend,
		logger:   logger.With("component", "lease_manager"),
	}
	activeLeasesFunc := func() int {
		return len(m.leaseMap)
	}
	m.metrics = metrics.NewLeaseMetrics(reg, activeLeasesFunc)
	return m
}

func (lm *LeaseManager) Grant(ttl int64) (*Lease, error) {
	if ttl <= 0 {
		return nil, errTTLNegative
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}
	start := time.Now()
	defer func() { lm.metrics.GrantDurationSec.Observe(time.Since(start).Seconds()) }()

	lease := &Lease{
		ID:           nextID(),
		ttl:          ttl,
		remainingTTL: ttl,
		expiry:       time.Now().Add(time.Second * time.Duration(ttl)),
		keys:         make(map[LeasedKey]struct{}),
	}

	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	if err := lm.unsafeSaveToLeaseMap(lease); err != nil {
		return nil, err
	}
	lm.unsafePushToHeap(lease)
	if err := lm.unsafePersistToBackend(lease); err != nil {
		return nil, err
	}

	lm.metrics.LeasesGranted.Inc()
	lm.logger.Info("lease granted",
		"id", lease.ID,
		"ttl", lease.ttl,
		"expiry", lease.expiry,
	)
	return lease, nil
}

// noop on LeaseNotFound
func (lm *LeaseManager) Revoke(id int64) {
	lm.rwlock.Lock()
	start := time.Now()
	defer func() { lm.metrics.RevokeDurationSec.Observe(time.Since(start).Seconds()) }()

	lease, ok := lm.leaseMap[id]
	if !ok {
		return
	}
	toDelete := make([][]byte, len(lease.keys))
	for lk := range lease.keys {
		toDelete = append(toDelete, []byte(lk.Key))
	}
	lm.unsafeRemoveFromLeaseMap(lease)
	lm.unsafeRemoveFromHeap(lease.ID)
	lm.rwlock.Unlock()

	if len(toDelete) > 0 {
		w := lm.store.NewWriter()
		for _, k := range toDelete {
			w.DeleteKey(k)
		}
		w.End()
		lm.logger.Info("lease revoked: removed attached keys",
			"id", lease.ID,
			"key_count", len(toDelete),
		)

		lm.metrics.KeysPerRevoke.Observe(float64(len(toDelete)))
	}

	// logoljuk, de nem kell hibával visszatértünk:
	// a kulcsokat már úgyis töröltük, és a régi lease-k úgyis törlődnek
	// a következő Restorenál
	if err := lm.unsafeRemoveFromBackend(lease); err != nil {
		lm.logger.Error("lease: failed to delete lease from backend",
			"id", id,
			"error", err,
		)
	}

	lm.metrics.LeasesRevoked.Inc()

	lm.logger.Info("lease revoked",
		"id", lease.ID,
	)
}

func (lm *LeaseManager) KeepAlive(id int64) (remainingTTL int64, err error) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	start := time.Now()
	defer func() { lm.metrics.KeepAliveDurationSec.Observe(time.Since(start).Seconds()) }()

	lease, ok := lm.leaseMap[id]
	if !ok {
		lm.metrics.KeepAliveErrors.Inc()
		return 0, errLeaseNotFound
	}

	newExpiry := time.Now().Add(time.Second * time.Duration(lease.ttl))
	rem := int64(time.Until(newExpiry).Seconds())

	lease.expiry = newExpiry
	lease.remainingTTL = rem

	lm.unsafeUpdateHeap(id, newExpiry)
	if err = lm.unsafePersistToBackend(lease); err != nil {
		lm.metrics.KeepAliveErrors.Inc()
		// logoljuk, de nem kell hibával visszatértünk:
		// a következő checkpointnál újrapróbálkozunk
		lm.logger.Warn("lease keepalive: persist failed, will retry on checkpoint",
			"id", id,
			"error", err,
		)
	}
	lm.metrics.KeepAliveTotal.Inc()
	return rem, nil
}

func (lm *LeaseManager) AttachKey(id int64, key []byte) error {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	lease, ok := lm.leaseMap[id]
	if !ok {
		return errLeaseNotFound
	}
	lease.keys[LeasedKey{Key: string(key)}] = struct{}{}
	lm.metrics.LeasedKeys.Inc()
	return nil
}

func (lm *LeaseManager) DetachKey(id int64, key []byte) error {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	lease, ok := lm.leaseMap[id]
	if !ok {
		return errLeaseNotFound
	}
	delete(lease.keys, LeasedKey{Key: string(key)})
	lm.metrics.LeasedKeys.Dec()
	return nil
}

func (lm *LeaseManager) Lookup(id int64) *Lease {
	lm.rwlock.RLock()
	defer lm.rwlock.RUnlock()
	return lm.leaseMap[id]
}

func (lm *LeaseManager) ApplyCheckpoints(cmd command.LeaseCheckpointCmd) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()

	for _, ck := range cmd.Checkpoints {
		lease, ok := lm.leaseMap[ck.ID]
		if !ok {
			continue
		}
		if ck.RemainingTTL <= 0 {
			continue
		}
		lease.remainingTTL = ck.RemainingTTL

		if err := lm.unsafePersistToBackend(lease); err != nil {
			// logoljuk, de nem kell hibával visszatértünk:
			// a következő checkpointnál újrapróbálkozunk
			// legrosszabb esetben ez a lease picit tovább él
			// a következő restore után
			lm.logger.Warn("checkpoint failed: failed to persist leave",
				"lease_id", lease.ID,
				"checkpoint_remainging_ttl_sec", ck.RemainingTTL,
				"error", err,
			)
		}
	}
}

func (lm *LeaseManager) genereteCheckpoints() command.LeaseCheckpointCmd {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	now := time.Now()
	checks := make([]command.Checkpoint, 0, len(lm.leaseMap))
	for id, lease := range lm.leaseMap {
		rem := int64(lease.expiry.Sub(now).Seconds())
		if rem <= 0 {
			continue
		}

		checks = append(checks, command.Checkpoint{
			ID:           id,
			RemainingTTL: rem,
		})
	}
	return command.LeaseCheckpointCmd{
		Checkpoints: checks,
	}
}

func (lm *LeaseManager) ExpiredLeases() (expired []*Lease) {
	lm.rwlock.Lock()
	defer lm.rwlock.Unlock()
	now := time.Now()
	expired = make([]*Lease, 0)
	for {
		_min := lm.heap.(*leaseHeap).peekMin()
		if _min == nil {
			return
		}
		if _min.time.Before(now) {
			item := lm.heap.Pop().(*heapItem)
			lm.unsafeRemoveFromLeaseMap(item.lease)
			expired = append(expired, item.lease)
		}
	}
}

func (lm *LeaseManager) Restore(newStore *mvcc.KVStore) error {
	util.Todo("LeaseManager.Restore")
	return nil
}

// privát helper függvények

// saveLeaseInmem elmenti a leaset a leaseMap-be
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeSaveToLeaseMap(l *Lease) error {
	if _, ok := lm.leaseMap[l.ID]; ok {
		return errLeaseIDConflict
	}
	lm.leaseMap[l.ID] = l
	return nil
}

// unsafeRemoveFromHeap eltávolítja a least a heapről O(log n) időben
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeRemoveFromHeap(id int64) {
	item, ok := lm.heapMap[id]
	if !ok {
		return
	}
	heap.Remove(lm.heap, item.index)
	delete(lm.heapMap, id)
}

// unsafePersistLease elmenti a leaset a backendbe
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafePersistToBackend(lease *Lease) error {
	bucketKey := EncodeLeaseBucketKey(LeaseBucketKey(lease.ID))
	leaseBytes, err := EncodeLease(lease)
	if err != nil {
		return fmt.Errorf("%w: %e", errLease, err)
	}

	wtx := lm.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	if err := wtx.UnsafePut(schema.BucketLeaseWIP, bucketKey, leaseBytes); err != nil {
		wtx.Abort()
		return fmt.Errorf("%v: %w", errLease, err)
	}
	if _, err := wtx.Commit(); err != nil {
		wtx.Abort()
		return fmt.Errorf("%v: %w", errLease, err)
	}

	return nil
}

func (lm *LeaseManager) unsafeRemoveFromBackend(lease *Lease) error {
	bucketKey := EncodeLeaseBucketKey(LeaseBucketKey(lease.ID))

	wtx := lm.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	if err := wtx.UnsafeDelete(schema.BucketLeaseWIP, bucketKey); err != nil {
		wtx.Abort()
		return fmt.Errorf("%v: %w", errLease, err)
	}
	if _, err := wtx.Commit(); err != nil {
		wtx.Abort()
		return fmt.Errorf("%v: %w", errLease, err)
	}

	return nil
}

// unsafeRemoveFromLeaseMap törli a leaset a leaseMap-ből
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeRemoveFromLeaseMap(l *Lease) {
	delete(lm.leaseMap, l.ID)
}

// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafePushToHeap(lease *Lease) {
	item := &heapItem{lease: lease, time: lease.expiry}
	heap.Push(lm.heap, item)
	lm.heapMap[lease.ID] = item
}

// unsafeUpdateHeap frissít egy már meglévő elemet
// heap.Fix O(log n) időben újraépíti a heap invaránsát
// NOTE: caller needs to hold the lock
func (lm *LeaseManager) unsafeUpdateHeap(id int64, t time.Time) {
	item, ok := lm.heapMap[id]
	if !ok {
		return
	}
	item.time = t
	heap.Fix(lm.heap, item.index)
}

// nextID új ID-t generál
// TODO: ID collision panics
func nextID() int64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		err = fmt.Errorf("%w: failed to generate ID: %v", errLease, err)
		panic(err)
	}
	id := int64(binary.BigEndian.Uint64(buf[:]))
	// 0 is reserved as "no lease"
	if id <= 0 {
		id = -id
	}
	return id
}
