package kv

import (
	"bytes"
	"fmt"
)

var ErrRevNotFound = fmt.Errorf("revision not found")
var ErrKeyIndexError = fmt.Errorf("key index error")

type keyIndex struct {
	key         []byte
	modRev      Revision
	generations []generation
}

func (ki *keyIndex) Less(bki *keyIndex) bool {
	return bytes.Compare(ki.key, bki.key) == -1
}

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

// get retrieves the createRev, modRev and version of the key
func (ki *keyIndex) get(targetRev int64) (createRev Revision, modRev Revision, version int64, err error) {
	if ki.isEmpty() {
		panic("get: getting empty key index")
	}

	g := ki.findGen(targetRev)
	if g.isEmpty() {
		return Revision{}, Revision{}, 0, fmt.Errorf("%w: get: generation is empty", ErrKeyIndexError)
	}

	n := g.walk(func(rev Revision) bool {
		return rev.Main > targetRev
	})

	if n != -1 {
		return g.created, g.revs[n], g.version - int64(len(g.revs)-n-1), nil
	}
	return Revision{}, Revision{}, 0, ErrRevNotFound
}

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

// reviveTombstone restores a tombstone value which is the only
// revision so far for a key. We dont know the createRev and version of the generation,
// so we just set them to 0 . The modRev is set to the given main and sub revision.
func (ki *keyIndex) reviveTombstone(main, sub int64) error {
	err := ki.doRevive(Revision{}, Revision{Main: main, Sub: sub}, 1)
	if err != nil {
		return err
	}
	ki.generations = append(ki.generations, generation{})
	return nil
}

func (ki *keyIndex) doRevive(createRev, modRev Revision, version int64) error {
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
func (ki *keyIndex) findGen(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg

	for cg >= 0 {
		if len(ki.generations[cg].revs) == 0 {
			cg--
			continue
		}
		g := ki.generations[cg]
		if cg != lastg {
			if tomb := g.revs[len(g.revs)-1].Main; tomb <= rev {
				return nil
			}
		}
		if g.revs[0].Main <= rev {
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
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
	var gi int
	// find the generations to start checking
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []Revision
	var last int64
	for ; gi < len(ki.generations); gi++ {
		for _, r := range ki.generations[gi].revs {
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

	revIndex = g.walk(f)

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

type generation struct {
	version int64
	created Revision
	revs    []Revision
}

func (g *generation) isEmpty() bool {
	return len(g.revs) == 0
}

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
func (g *generation) walk(f func(rev Revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
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
