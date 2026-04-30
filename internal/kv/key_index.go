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
	"bytes"
	"fmt"
)

var ErrRevNotFound = fmt.Errorf("revision not found")
var ErrKeyIndexError = fmt.Errorf("key index error")

// keyIndex tracks the complete revision history of a single user key.
//
// It is stored as a pointer in the treeIndex B-tree, sorted by `key` bytes.
// The B-tree never stores the user key as a bucket key — it only uses it
// for in-memory lookups. The actual data lives in BucketMain, keyed by
// BucketKey (main, sub, tombstone).
//
// Structure:
//
//	keyIndex{
//	  key:         []byte("mykey"),      // the user's key, used for B-tree ordering
//	  modRev:      {Main: 5, Sub: 0},    // most recent revision (across all generations)
//	  generations: []generation{         // one generation per create-delete cycle
//	    {created: {1,0}, version: 3, revs: [{1,0}, {3,0}, {5,0}]},  // active
//	  },
//	}
//
// Lifecycle of a key:
//
//	PUT "foo"  → generation[0] created, rev {1,0} appended
//	PUT "foo"  → rev {3,0} appended to generation[0], version bumped
//	DEL "foo"  → rev {5,0} appended as tombstone, new empty generation[1] started
//	PUT "foo"  → generation[1] gets its first rev, key is "re-created"
//
// Why generations?
//
//	A key can be created, deleted, and re-created multiple times.
//	Each create-to-delete cycle is one generation. Compaction can remove
//	entire old generations once they're fully below the compaction revision.
type keyIndex struct {
	key         []byte       // user key
	modRev      Revision     // latest modRev
	generations []generation // ordered list of create-delete cycles
}

func (ki *keyIndex) Less(b *keyIndex) bool {
	return bytes.Compare(ki.key, b.key) == -1
}

// put appends a new revision to the current (latest) generation.
//
// If there are no generations yet (brand new key), a new generation is created.
// The revision must be strictly greater than modRev — revisions are monotonically increasing.
//
// This is called for both PUT and DELETE operations. For DELETE, tombstone()
// calls put() first, then starts a new empty generation.
func (ki *keyIndex) put(main, sub int64) error {
	rev := Revision{
		Main: main,
		Sub:  sub,
	}

	if !rev.GreaterThan(ki.modRev) {
		return fmt.Errorf("%w: put with unexpected smaller version", ErrKeyIndexError)
	}

	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}

	g := &ki.generations[len(ki.generations)-1]
	if len(g.revs) == 0 {
		g.created = rev
	}
	g.revs = append(g.revs, rev)
	g.version++
	ki.modRev = rev
	return nil
}

// get retrieves the state of the key at a given revision.
// It finds the generation the revision is from, then walk back nutil it
// finds the biggest revision <= targetRev
func (ki *keyIndex) get(targetRev int64) (createRev Revision, modRev Revision, version int64, err error) {
	if ki.isEmpty() {
		panic("get: getting empty key index")
	}

	g, err := ki.findGen(targetRev)
	if err != nil {
		return Revision{}, Revision{}, 0, fmt.Errorf("%w: get: %w", ErrKeyNotFound, err)
	}

	n := g.walkBackwards(func(rev Revision) bool {
		return rev.Main > targetRev
	})

	if n != -1 {
		return g.created, g.revs[n], g.version - int64(len(g.revs)-n-1), nil
	}
	return Revision{}, Revision{}, 0, ErrRevNotFound
}

// tombstone marks the key as deleted at the given revision
// by appending the current revision to the current gen, then creating an empty generation
// marking the end of the keys life
//
// in the main bucket the tombstone marked by a [T] appended after the key like [8 bytes][8 bytes][T]
func (ki *keyIndex) tombstone(main, sub int64) error {
	if ki.isEmpty() {
		return fmt.Errorf("%w: delete: placing tombstone on empty key index", ErrKeyIndexError)
	}

	if ki.generations[len(ki.generations)-1].isEmpty() {
		return fmt.Errorf("%w: delete: placing tombstone on empty generation", ErrKeyIndexError)
	}

	err := ki.put(main, sub)
	if err != nil {
		return err
	}
	ki.generations = append(ki.generations, generation{})
	return nil
}

// restoreTombstone restores a tombstone value which is the only
// revision so far for a key. We dont know the createRev and version of the generation,
// so we just set them to 0 . The modRev is set to the given main and sub revision.
func (ki *keyIndex) restoreTombstone(main, sub int64) error {
	err := ki.doRestore(Revision{}, Revision{Main: main, Sub: sub}, 1)
	if err != nil {
		return err
	}
	ki.generations = append(ki.generations, generation{})
	return nil
}

// doRestore creates a generation with a singe rev
func (ki *keyIndex) doRestore(createRev, modRev Revision, version int64) error {
	if len(ki.generations) != 0 {
		return fmt.Errorf("revive: placing tombstone on non-empty key index")
	}

	ki.modRev = modRev
	g := generation{
		version: version,
		created: createRev,
		revs:    []Revision{modRev},
	}
	ki.generations = append(ki.generations, g)
	return nil
}

// findGen finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
func (ki *keyIndex) findGen(rev int64) (*generation, error) {
	lastGen := len(ki.generations) - 1
	currentGen := lastGen

	for currentGen >= 0 {
		if len(ki.generations[currentGen].revs) == 0 {
			currentGen--
			continue
		}
		g := ki.generations[currentGen]
		if currentGen != lastGen {
			if tomb := g.revs[len(g.revs)-1].Main; tomb <= rev {
				return nil, fmt.Errorf("tombstone at %d", tomb)
			}
		}
		if g.revs[0].Main <= rev {
			return &ki.generations[currentGen], nil
		}
		currentGen--
	}
	return nil, fmt.Errorf("revision is at the gap of two generations")
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && len(ki.generations[0].revs) == 0
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
func (ki *keyIndex) since(rev int64) ([]Revision, error) {
	if ki.isEmpty() {
		return nil, fmt.Errorf("'since' got an unexpected empty keyIndex: %s", string(ki.key))
	}
	since := Revision{Main: rev}
	var idxGen int
	// find the generations to start checking
	for idxGen = len(ki.generations) - 1; idxGen > 0; idxGen-- {
		g := ki.generations[idxGen]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []Revision
	var last int64
	for ; idxGen < len(ki.generations); idxGen++ {
		for _, r := range ki.generations[idxGen].revs {
			if since.GreaterThan(r) {
				continue
			}
			if r.Main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r)
			last = r.Main
		}
	}

	return revs, nil
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one.
// If a generation becomes empty during compaction, it will be removed.
func (ki *keyIndex) compact(atRev int64, available map[Revision]struct{}) error {
	if ki.isEmpty() {
		return fmt.Errorf("'compact' got an unexpected empty keyIndex: %s", string(ki.key))

	}

	genIdx, revIdx := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove the previous contents.
		if revIdx != -1 {
			g.revs = g.revs[revIdx:]
		}
	}

	// remove the previous generations.
	ki.generations = ki.generations[genIdx:]
	return nil
}

func (ki *keyIndex) doCompact(atRev int64, available map[Revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	f := func(rev Revision) bool {
		if rev.Main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].Main; tomb >= atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	revIndex = g.walkBackwards(f)

	return genIdx, revIndex
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(targetRev int64, available map[Revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(targetRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// If the given `targetRev` is a tombstone, we need to skip it.
		//
		// Note that this s different from the `compact` function which
		// keeps tombstone in such case. We need to stay consistent with
		// existing versions, ensuring they always generate the same hash
		// values.
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modRev != b.modRev {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) dropRev(rev Revision) {
	if len(ki.generations) == 0 {
		return
	}

	lastGenIdx := len(ki.generations) - 1
	g := &ki.generations[lastGenIdx]

	if len(g.revs) == 0 {
		// if tombstoned, remove empty generation
		ki.generations = ki.generations[:lastGenIdx]
		lastGenIdx--
		if lastGenIdx < 0 {
			ki.modRev = Revision{}
			return
		}
		g = &ki.generations[lastGenIdx]
	}

	// pop revs that match
	if len(g.revs) > 0 && g.revs[len(g.revs)-1] == rev {
		g.revs = g.revs[:len(g.revs)-1]
		g.version--
	}

	var newModRev Revision
	for i := len(ki.generations) - 1; i >= 0; i-- {
		if len(ki.generations[i].revs) > 0 {
			newModRev = ki.generations[i].revs[len(ki.generations[i].revs)-1]
			break
		}
	}
	ki.modRev = newModRev // get the updated modRev
}

type generation struct {
	version int64
	created Revision
	revs    []Revision
}

func (g *generation) isEmpty() bool {
	return g == nil || len(g.revs) == 0
}

// walkBackwards walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walkBackwards returns until: 1. it finishes walking all pairs 2. the function returns false.
// walkBackwards returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
func (g *generation) walkBackwards(f func(rev Revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
}

func (g *generation) walk(f func(rev Revision) error) error {
	for _, r := range g.revs {
		if err := f(r); err != nil {
			return err
		}
	}
	return nil
}

// equal returns true if the two generations are identical, false otherwise.
func (g generation) equal(b generation) bool {
	if g.version != b.version {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
