// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modified from the etcd codebase for thesis purposes.

package kv

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/btree"
)

var ErrCompactionFailed = fmt.Errorf("treeIndex: compaction failed")

// taken from etcd.io/etcd/server/storage/mvcc/index.go

//	key -> {
//		key,
//		{key, lastModRev, []revs},
//	}
type Index interface {
	// Get looks up a single user key at a target revision.
	// Returns the matching revision, the create revision, and the version counter.
	// Returns ErrRevNotFound if the key doesn't exist at that revision.
	Get(key []byte, atRev int64) (createRev, modRev Revision, ver int64, err error)

	// Put records that a user key was written at the given revision.
	// Creates a new keyIndex if the key has never been seen before.
	Put(key []byte, rev Revision) error

	// Range returns all user keys in [key, end) that are live at targetRev.
	// If end is nil, returns only the exact key match.
	Range(key, end []byte, targetRev int64) ([][]byte, []Revision)

	// RevisionRange returns all {revision,tombstone} found for the  key range[key, end)
	// in the  range [startRev, endRev).
	RevisionsRange(key, end []byte, startRev, endRev int64) []KvBucketKey

	// Revisions returns limited number of revisions from [key, end) at the given rev.
	// The returned slice is sorted in the order of key. There is no limit if limit <= 0.
	// The second return parameter isn't capped by the limit and reflects the total number of revisions.
	Revisions(key, end []byte, targetRev int64, limit int) (revs []Revision, total int)

	// CountRevisions counts how many keys in [key, end) are live at targetRev.
	CountRevisions(key, end []byte, targetRev int64) int

	// Tombstone marks a user key as deleted at the given revision.
	// This ends the current generation and starts a new empty one.
	Tombstone(key []byte, rev Revision) error

	// Compact removes all revision history at or below `rev` that is superseded.
	// Returns the set of revisions that must be KEPT in BucketMain (the latest
	// revision per generation that is <= rev). Everything else below rev can be
	// deleted from the backend.
	Compact(rev int64) (map[Revision]struct{}, error)

	// Clear removes all entries. Used during Restore() before rebuilding.
	Clear()

	// Keep is like Compact but read-only — it computes which revisions would
	// be kept without actually modifying the index.
	Keep(rev int64) map[Revision]struct{}

	// Equal checks structural equality with another Index. Used in tests.
	Equal(b Index) bool

	// Insert directly inserts a keyIndex. Used during Restore() to rebuild
	// the index from BucketMain entries.
	Insert(ki *keyIndex)

	// KeyIndex looks up a keyIndex by key. Returns nil if not found.
	KeyIndex(ki *keyIndex) *keyIndex

	// DropRevision drops the given revision for the key if found
	DropRevision(key []byte, rev Revision)
}

type treeIndex struct {
	mu     sync.RWMutex
	tree   *btree.BTreeG[*keyIndex]
	logger *slog.Logger
}

func NewTreeIndex(logger *slog.Logger) Index {
	tree := btree.NewG(32, func(a, b *keyIndex) bool {
		return a.Less(b)
	})
	return &treeIndex{
		tree:   tree,
		logger: logger.With("component", "tree_index"),
	}
}

func (ti *treeIndex) Get(key []byte, targetRev int64) (createRev, modRev Revision, version int64, err error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return ti.unsafeGet(key, targetRev)
}

// unsafeGet does not lock the resouce while reading it
func (ti *treeIndex) unsafeGet(key []byte, targetRev int64) (createRev, modRev Revision, version int64, err error) {
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

// RevisionRange returns all revisions found for the key [key, end) in the range [startRev, endRev).
func (ti *treeIndex) RevisionsRange(key, end []byte, startRev, endRev int64) (out []KvBucketKey) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	getRevs := func(ki *keyIndex) {
		for i, g := range ki.generations {
			genComplete := i < len(ki.generations)-1
			lastRevIdx := len(g.revs) - 1
			_ = g.walk(func(rev Revision) error {
				if rev.Main > startRev && rev.Main < endRev {
					tomb := genComplete && g.revs[lastRevIdx] == rev
					out = append(out, KvBucketKey{Revision: rev, Tombstone: tomb})
				}
				return nil
			})
		}
	}

	if startRev > endRev {
		return
	}

	if end == nil && len(key) != 0 {
		ki := &keyIndex{key: key}
		if ki = ti.keyIndex(ki); ki == nil {
			return make([]KvBucketKey, 0)
		}
		getRevs(ki)
		return
	}

	if key == nil {
		key = []byte{}
	}

	// didnt check prev value, but all tests passed:
	// unsafeVisit + getRevs handles this case too
	// if end == nil && len(key) != 0 {
	// 	ti.unsafeGet(key, endRev)
	// }

	ti.unsafeVisit(key, end, func(ki *keyIndex) bool {
		getRevs(ki)
		return true
	})

	return out
}

// Revisions returns limited number of revisions from key(included) to end(excluded)
// at the given rev. The returned slice is sorted in the order of key. There is no limit if limit <= 0.
// The second return parameter isn't capped by the limit and reflects the total number of revisions.
func (ti *treeIndex) Revisions(key, end []byte, targetRev int64, limit int) (revs []Revision, total int) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if end == nil && len(key) != 0 {
		_, modRev, _, err := ti.unsafeGet(key, targetRev)
		if err != nil {
			return nil, 0
		}
		return []Revision{modRev}, 1
	}

	if key == nil {
		key = []byte{}
	}

	ti.unsafeVisit(key, end, func(ki *keyIndex) bool {
		if _, mod, _, err := ki.get(targetRev); err == nil {
			if limit <= 0 || len(revs) < limit {
				revs = append(revs, mod)
			}
			total++
		}
		return true
	})
	return
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
		if err == nil {
			total++
		}
		return true
	})
	return total
}

func (ti *treeIndex) Range(key, end []byte, targetRev int64) (keys [][]byte, revs []Revision) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

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
		err = ki.compact(rev, avail)
		if err != nil {
			err = fmt.Errorf("failed to compact key index: %w", err)
			ti.mu.Unlock()
			return false
		}
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

func (a *treeIndex) Equal(i Index) bool {
	b := i.(*treeIndex)

	if a.tree.Len() != b.tree.Len() {
		return false
	}

	eq := true
	a.tree.Ascend(func(thisKi *keyIndex) bool {
		thatKi, _ := b.tree.Get(thisKi)
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

func (ti *treeIndex) Clear() {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.tree.Clear(false)
}

func (ti *treeIndex) DropRevision(key []byte, rev Revision) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	ki := &keyIndex{key: key}
	if got, ok := ti.tree.Get(ki); ok {
		got.dropRev(rev)
		if got.isEmpty() {
			ti.tree.Delete(got)
		}
	}
}
