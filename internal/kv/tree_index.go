package kv

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/btree"
)

var ErrCompactionFailed = fmt.Errorf("treeIndex: compaction failed")

// taken from etcd.io/etcd/server/storage/mvcc/index.go

type index interface {
	Get(key []byte, atRev int64) (rev, created Revision, ver int64, err error)
	Put(key []byte, rev Revision) error
	Range(key, end []byte, atRev int64) ([][]byte, []Revision)
	Revisions(key, end []byte, atRev int64, limit int) ([]Revision, int)
	CountRevisions(key, end []byte, atRev int64) int
	Tombstone(key []byte, rev Revision) error
	Compact(rev int64) (map[Revision]struct{}, error)
	Keep(rev int64) map[Revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

func newTreeIndex(logger *slog.Logger) index {
	tree := btree.NewG[*keyIndex](32, func(a, b *keyIndex) bool {
		return a.Less(b)
	})
	return &treeIndex{
		tree:   tree,
		logger: logger.With("component", "treeIndex"),
	}
}

type treeIndex struct {
	mu     sync.RWMutex
	tree   *btree.BTreeG[*keyIndex]
	logger *slog.Logger
}

func (ti *treeIndex) Get(key []byte, targetRev int64) (rev, created Revision, version int64, err error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return ti.unsafeGet(key, targetRev)
}

// unsafeGet does not lock the resouce while reading it
func (ti *treeIndex) unsafeGet(key []byte, targetRev int64) (rev, created Revision, version int64, err error) {
	ki := &keyIndex{key: key}
	if ki = ti.keyIndex(ki); ki == nil {
		return Revision{}, Revision{}, 0, ErrRevNotFound
	}
	return ki.get(targetRev)
}

func (ti *treeIndex) Put(key []byte, rev Revision) error {
	insertKi := &keyIndex{key: key}
	ti.mu.Lock()
	defer ti.mu.Unlock()

	oldKi, ok := ti.tree.Get(insertKi)
	if !ok {
		err := insertKi.put(rev.Main, rev.Sub)
		if err != nil {
			return err
		}

		ti.tree.ReplaceOrInsert(insertKi)
		return nil
	}

	return oldKi.put(rev.Main, rev.Sub)
}

func (ti *treeIndex) KeyIndex(ki *keyIndex) *keyIndex {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return ti.keyIndex(ki)
}

func (ti *treeIndex) keyIndex(ki *keyIndex) *keyIndex {
	if got, ok := ti.tree.Get(ki); ok {
		return got
	}
	return nil
}

// unsafeVisit applies f to each item in the tree between in the range [key, end)
// wihtout locking the resource
func (ti *treeIndex) unsafeVisit(key, end []byte, f func(ki *keyIndex) bool) {
	startKi, endKi := &keyIndex{key: key}, &keyIndex{key: end}

	ti.tree.AscendGreaterOrEqual(startKi, func(item *keyIndex) bool {
		if len(endKi.key) > 0 && !item.Less(endKi) {
			return false
		}
		if !f(item) {
			return false
		}
		return true
	})
}

// Revisions returns limited number of revisions from key(included) to end(excluded)
// at the given rev. The returned slice is sorted in the order of key. There is no limit if limit <= 0.
// The second return parameter isn't capped by the limit and reflects the total number of revisions.
func (ti *treeIndex) Revisions(key, end []byte, targetRev int64, limit int) (revs []Revision, total int) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if end == nil {
		rev, _, _, err := ti.unsafeGet(key, targetRev)
		if err != nil {
			return nil, 0
		}
		return []Revision{rev}, 1
	}
	ti.unsafeVisit(key, end, func(ki *keyIndex) bool {
		if rev, _, _, err := ki.get(targetRev); err == nil {
			if limit <= 0 || len(revs) < limit {
				revs = append(revs, rev)
			}
			total++
		}
		return true
	})
	return revs, total
}

// CountRevisions returns the number of revisions
// from key(included) to end(excluded) at the given rev.
func (ti *treeIndex) CountRevisions(key, end []byte, targetRev int64) int {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if end == nil {
		_, _, _, err := ti.unsafeGet(key, targetRev)
		if err != nil {
			return 0
		}
		return 1
	}

	total := 0
	ti.unsafeVisit(key, end, func(ki *keyIndex) bool {
		_, _, _, err := ki.get(targetRev)
		if err != nil {
			total++
		}
		return true
	})
	return total
}

func (ti *treeIndex) Range(key, end []byte, targetRev int64) (keys [][]byte, revs []Revision) {
	ti.mu.RLock()
	defer ti.mu.Unlock()

	if end == nil {
		createRev, _, _, err := ti.unsafeGet(key, targetRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []Revision{createRev}
	}

	ti.unsafeVisit(key, end, func(ki *keyIndex) bool {
		createRev, _, _, err := ki.get(targetRev)
		if err == nil {
			revs = append(revs, createRev)
			keys = append(keys, ki.key)
		}
		return true
	})
	return keys, revs
}

func (ti *treeIndex) Tombstone(key []byte, rev Revision) error {
	ki := &keyIndex{key: key}
	ti.mu.Lock()
	defer ti.mu.Unlock()

	got, ok := ti.tree.Get(ki)
	if !ok {
		return ErrRevNotFound
	}

	return got.tombstone(rev.Main, rev.Sub)
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[Revision]struct{} {
	avail := make(map[Revision]struct{})
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	ti.tree.Ascend(func(keyi *keyIndex) bool {
		keyi.keep(rev, avail)
		return true
	})
	return avail
}

func (ti *treeIndex) Compact(rev int64) (avail map[Revision]struct{}, err error) {
	avail = make(map[Revision]struct{})
	ti.logger.Info("compacting tree index", "revision", rev)
	ti.mu.Lock()
	clone := ti.tree.Clone()
	ti.mu.Unlock()

	clone.Ascend(func(ki *keyIndex) bool {
		// lock the resource only when we need to modify it
		ti.mu.Lock()
		ki.compact(rev, avail)
		if ki.isEmpty() {
			_, ok := ti.tree.Delete(ki)
			if !ok {
				err = fmt.Errorf("%w: failed to delete key index", ErrCompactionFailed)
				ti.mu.Unlock()
				return false
			}
		}
		ti.mu.Unlock()
		return true
	})

	return avail, err
}

func (this *treeIndex) Equal(i index) bool {
	that := i.(*treeIndex)

	if this.tree.Len() != that.tree.Len() {
		return false
	}

	eq := true
	this.tree.Ascend(func(thisKi *keyIndex) bool {
		thatKi, _ := that.tree.Get(thisKi)
		if !thisKi.equal(thatKi) {
			eq = false
			return false
		}
		return true
	})

	return eq
}

func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}
