package kv

import (
	"fmt"
	"reflect"
	"testing"
)

// ==================== generation tests ====================

func Test_GenerationIsEmpty(t *testing.T) {
	tests := []struct {
		name string
		g    generation
		want bool
	}{
		{"nil revs", generation{}, true},
		{"empty revs", generation{revs: []Revision{}}, true},
		{"one rev", generation{revs: []Revision{{1, 0}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.g.isEmpty(); got != tt.want {
				t.Errorf("isEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_GenerationWalkBackwards(t *testing.T) {
	g := generation{revs: []Revision{{2, 0}, {4, 0}, {6, 0}}}

	// Walk until we find rev <= 5 (should stop at index 1 → rev {4,0})
	idx := g.walkBackwards(func(rev Revision) bool {
		return rev.Main > 5
	})
	if idx != 1 {
		t.Errorf("walkBackwards stopped at %d, want 1", idx)
	}

	// Walk until we find rev <= 1 → exhausts all revs
	idx = g.walkBackwards(func(rev Revision) bool {
		return rev.Main > 1
	})
	if idx != -1 {
		t.Errorf("walkBackwards stopped at %d, want -1", idx)
	}

	// Walk and stop immediately (everything <= 10)
	idx = g.walkBackwards(func(rev Revision) bool {
		return rev.Main > 10
	})
	if idx != 2 {
		t.Errorf("walkBackwards stopped at %d, want 2", idx)
	}
}

func Test_GenerationEqual(t *testing.T) {
	g1 := generation{version: 2, created: Revision{1, 0}, revs: []Revision{{1, 0}, {3, 0}}}
	g2 := generation{version: 2, created: Revision{1, 0}, revs: []Revision{{1, 0}, {3, 0}}}
	g3 := generation{version: 3, created: Revision{1, 0}, revs: []Revision{{1, 0}, {3, 0}}}
	g4 := generation{version: 2, created: Revision{1, 0}, revs: []Revision{{1, 0}, {5, 0}}}

	if !g1.equal(g2) {
		t.Error("identical generations should be equal")
	}
	if g1.equal(g3) {
		t.Error("different versions should not be equal")
	}
	if g1.equal(g4) {
		t.Error("different revisions should not be equal")
	}
}

// ==================== keyIndex.put tests ====================

func Test_KeyIndexPut(t *testing.T) {
	ki := &keyIndex{key: []byte("foo")}

	// First put creates generation with created revision
	if err := ki.put(2, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantKI := &keyIndex{
		key:    []byte("foo"),
		modRev: Revision{2, 0},
		generations: []generation{
			{version: 1, created: Revision{2, 0}, revs: []Revision{{2, 0}}},
		},
	}
	if !ki.equal(wantKI) {
		t.Errorf("after first put: ki = %+v, want %+v", ki, wantKI)
	}

	// Second put appends to same generation
	if err := ki.put(4, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ki.modRev != (Revision{4, 0}) {
		t.Errorf("modRev = %v, want {4,0}", ki.modRev)
	}
	if ki.generations[0].version != 2 {
		t.Errorf("version = %d, want 2", ki.generations[0].version)
	}
	if len(ki.generations[0].revs) != 2 {
		t.Errorf("revs len = %d, want 2", len(ki.generations[0].revs))
	}

	// Put with sub revisions (same main rev, different sub)
	if err := ki.put(5, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ki.put(5, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ki.generations[0].version != 4 {
		t.Errorf("version = %d, want 4", ki.generations[0].version)
	}
}

func Test_KeyIndexPutRejectsOlderRevision(t *testing.T) {
	ki := &keyIndex{key: []byte("foo")}
	ki.put(5, 0)

	err := ki.put(3, 0)
	if err == nil {
		t.Fatal("expected error for older revision, got nil")
	}

	err = ki.put(5, 0) // same revision, not greater
	if err == nil {
		t.Fatal("expected error for equal revision, got nil")
	}
}

// ==================== keyIndex.get tests ====================

// Helper: build a keyIndex with a known lifecycle:
//
//	PUT at {2,0}, PUT at {4,0}, PUT at {6,0}  — generation 0
//	DEL at {8,0}                               — tombstone, starts generation 1
//	PUT at {10,0}, PUT at {12,0}               — generation 1
//	DEL at {14,0}                              — tombstone, starts generation 2
//	PUT at {16,0}                              — generation 2 (active)
func newTest_KeyIndex() *keyIndex {
	ki := &keyIndex{key: []byte("foo")}
	ki.put(2, 0)
	ki.put(4, 0)
	ki.put(6, 0)
	ki.tombstone(8, 0)
	ki.put(10, 0)
	ki.put(12, 0)
	ki.tombstone(14, 0)
	ki.put(16, 0)
	return ki
}

func Test_KeyIndexGet(t *testing.T) {
	ki := newTest_KeyIndex()

	tests := []struct {
		name       string
		rev        int64
		wantCreate Revision
		wantMod    Revision
		wantVer    int64
		wantErr    bool
	}{
		// Generation 0: created at 2, revs [2,4,6], tombstone at 8
		{"at rev 2 (first put)", 2, Revision{2, 0}, Revision{2, 0}, 1, false},
		{"at rev 3 (between puts)", 3, Revision{2, 0}, Revision{2, 0}, 1, false},
		{"at rev 4 (second put)", 4, Revision{2, 0}, Revision{4, 0}, 2, false},
		{"at rev 5 (between puts)", 5, Revision{2, 0}, Revision{4, 0}, 2, false},
		{"at rev 6 (third put)", 6, Revision{2, 0}, Revision{6, 0}, 3, false},
		{"at rev 7 (before tombstone)", 7, Revision{2, 0}, Revision{6, 0}, 3, false},

		// Rev 8 is a tombstone — the key was alive at rev 7 but dead at rev 8
		// findGen at rev 8: gen0's tombstone is rev 8, which is <= 8, so it's a gap
		{"at rev 8 (tombstone — gap)", 8, Revision{}, Revision{}, 0, true},
		{"at rev 9 (between tombstone and re-create)", 9, Revision{}, Revision{}, 0, true},

		// Generation 1: created at 10, revs [10,12], tombstone at 14
		{"at rev 10 (re-create)", 10, Revision{10, 0}, Revision{10, 0}, 1, false},
		{"at rev 12 (second put gen1)", 12, Revision{10, 0}, Revision{12, 0}, 2, false},
		{"at rev 13 (between put and tombstone)", 13, Revision{10, 0}, Revision{12, 0}, 2, false},

		// Rev 14 is tombstone of gen1
		{"at rev 14 (tombstone gen1)", 14, Revision{}, Revision{}, 0, true},
		{"at rev 15 (gap before gen2)", 15, Revision{}, Revision{}, 0, true},

		// Generation 2: created at 16 (active)
		{"at rev 16 (active gen)", 16, Revision{16, 0}, Revision{16, 0}, 1, false},
		{"at rev 100 (far future, active gen)", 100, Revision{16, 0}, Revision{16, 0}, 1, false},

		// Before key ever existed
		{"at rev 1 (before key existed)", 1, Revision{}, Revision{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created, mod, ver, err := ki.get(tt.rev)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil (created=%v, mod=%v, ver=%d)", created, mod, ver)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if created != tt.wantCreate {
				t.Errorf("created = %v, want %v", created, tt.wantCreate)
			}
			if mod != tt.wantMod {
				t.Errorf("mod = %v, want %v", mod, tt.wantMod)
			}
			if ver != tt.wantVer {
				t.Errorf("ver = %d, want %d", ver, tt.wantVer)
			}
		})
	}
}

func Test_KeyIndexGetPanicsOnEmpty(t *testing.T) {
	ki := &keyIndex{key: []byte("foo"), generations: []generation{{}}}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on get from empty keyIndex")
		}
	}()
	ki.get(1)
}

// ==================== keyIndex.tombstone tests ====================

func Test_KeyIndexTombstone(t *testing.T) {
	ki := &keyIndex{key: []byte("foo")}
	ki.put(5, 0)

	if err := ki.tombstone(7, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 generations: [{5,7}, {}]
	if len(ki.generations) != 2 {
		t.Fatalf("generations len = %d, want 2", len(ki.generations))
	}
	if ki.modRev != (Revision{7, 0}) {
		t.Errorf("modRev = %v, want {7,0}", ki.modRev)
	}
	// First gen has 2 revs (put + tombstone)
	if len(ki.generations[0].revs) != 2 {
		t.Errorf("gen[0].revs len = %d, want 2", len(ki.generations[0].revs))
	}
	// Second gen is empty (ready for re-creation)
	if !ki.generations[1].isEmpty() {
		t.Error("gen[1] should be empty")
	}

	// Can put again after tombstone (re-creates the key)
	if err := ki.put(8, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ki.generations[1].created != (Revision{8, 0}) {
		t.Errorf("gen[1].created = %v, want {8,0}", ki.generations[1].created)
	}
}

func Test_KeyIndexTombstoneOnEmpty(t *testing.T) {
	ki := &keyIndex{key: []byte("foo"), generations: []generation{{}}}
	err := ki.tombstone(1, 0)
	if err == nil {
		t.Error("expected error tombstoning empty keyIndex")
	}
}

func Test_KeyIndexTombstoneOnEmptyGeneration(t *testing.T) {
	ki := &keyIndex{key: []byte("foo")}
	ki.put(1, 0)
	ki.tombstone(2, 0)

	// Now gen[1] is empty. Tombstoning again should fail
	err := ki.tombstone(3, 0)
	if err == nil {
		t.Error("expected error tombstoning an already-empty generation")
	}
}

// ==================== keyIndex.isEmpty tests ====================

func Test_KeyIndexIsEmpty(t *testing.T) {
	tests := []struct {
		name string
		ki   keyIndex
		want bool
	}{
		{
			"one empty generation",
			keyIndex{key: []byte("foo"), generations: []generation{{}}},
			true,
		},
		{
			"one generation with data",
			keyIndex{key: []byte("foo"), generations: []generation{{revs: []Revision{{1, 0}}}}},
			false,
		},
		{
			"two generations (tombstoned)",
			keyIndex{key: []byte("foo"), generations: []generation{
				{revs: []Revision{{1, 0}, {2, 0}}},
				{},
			}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ki.isEmpty(); got != tt.want {
				t.Errorf("isEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ==================== keyIndex.Less tests ====================

func Test_KeyIndexLess(t *testing.T) {
	tests := []struct {
		a, b []byte
		want bool
	}{
		{[]byte("a"), []byte("b"), true},
		{[]byte("b"), []byte("a"), false},
		{[]byte("a"), []byte("a"), false},
		{[]byte(""), []byte("a"), true},
		{[]byte("foo"), []byte("foobar"), true},
	}
	for _, tt := range tests {
		ka := &keyIndex{key: tt.a}
		kb := &keyIndex{key: tt.b}
		if got := ka.Less(kb); got != tt.want {
			t.Errorf("Less(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

// ==================== keyIndex.findGen tests ====================

func Test_KeyIndexFindGen(t *testing.T) {
	ki := newTest_KeyIndex()

	tests := []struct {
		rev     int64
		wantNil bool
		wantRev int64 // first rev of the expected generation
	}{
		{1, true, 0},    // before key existed
		{2, false, 2},   // gen0
		{6, false, 2},   // gen0
		{8, true, 0},    // tombstone gap
		{9, true, 0},    // gap between gen0 and gen1
		{10, false, 10}, // gen1
		{14, true, 0},   // tombstone gap
		{15, true, 0},   // gap between gen1 and gen2
		{16, false, 16}, // gen2
		{99, false, 16}, // still gen2
	}

	for _, tt := range tests {
		g := ki.findGen(tt.rev)
		if tt.wantNil {
			if g != nil {
				t.Errorf("findGen(%d) = non-nil, want nil", tt.rev)
			}
		} else {
			if g == nil {
				t.Errorf("findGen(%d) = nil, want gen starting at rev %d", tt.rev, tt.wantRev)
			} else if g.revs[0].Main != tt.wantRev {
				t.Errorf("findGen(%d).revs[0].Main = %d, want %d", tt.rev, g.revs[0].Main, tt.wantRev)
			}
		}
	}
}

// ==================== keyIndex.since tests ====================

func Test_KeyIndexSince(t *testing.T) {
	ki := newTest_KeyIndex()

	tests := []struct {
		rev  int64
		want []Revision
	}{
		{1, []Revision{{2, 0}, {4, 0}, {6, 0}, {8, 0}, {10, 0}, {12, 0}, {14, 0}, {16, 0}}},
		{2, []Revision{{2, 0}, {4, 0}, {6, 0}, {8, 0}, {10, 0}, {12, 0}, {14, 0}, {16, 0}}},
		{3, []Revision{{4, 0}, {6, 0}, {8, 0}, {10, 0}, {12, 0}, {14, 0}, {16, 0}}},
		{6, []Revision{{6, 0}, {8, 0}, {10, 0}, {12, 0}, {14, 0}, {16, 0}}},
		{10, []Revision{{10, 0}, {12, 0}, {14, 0}, {16, 0}}},
		{16, []Revision{{16, 0}}},
		{17, nil},
	}

	for _, tt := range tests {
		got, err := ki.since(tt.rev)
		if err != nil {
			t.Fatalf("since(%d): unexpected error: %v", tt.rev, err)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("since(%d) = %v, want %v", tt.rev, got, tt.want)
		}
	}
}

// ==================== keyIndex.compact tests ====================

func Test_KeyIndexCompact(t *testing.T) {
	tests := []struct {
		compact   int64
		wantGens  int
		wantAvail map[Revision]struct{}
	}{
		// Compact at rev 1: before any key exists, nothing removed
		{1, 3, map[Revision]struct{}{}},
		// Compact at rev 2: gen0 keeps rev{2,0}, all 3 gens remain
		{2, 3, map[Revision]struct{}{{2, 0}: {}}},
		// Compact at rev 4: gen0 trimmed, keeps {4,0}
		{4, 3, map[Revision]struct{}{{4, 0}: {}}},
		// Compact at rev 6: gen0 trimmed, keeps {6,0}
		{6, 3, map[Revision]struct{}{{6, 0}: {}}},
		// Compact at rev 8: tombstone of gen0. gen0 trimmed to [{8,0}]. All 3 gens remain.
		{8, 3, map[Revision]struct{}{{8, 0}: {}}},
		// Compact at rev 9: gen0's tombstone is 8 < 9, so gen0 is skipped entirely.
		// doCompact lands on gen1. walkBackwards on gen1 can't find rev <= 9
		// because gen1 starts at 10. So revIdx = -1, nothing trimmed inside gen1.
		// gen0 is removed. 2 gens remain.
		{9, 2, map[Revision]struct{}{}},
		// Compact at rev 10: gen0 removed, gen1 keeps {10,0}
		{10, 2, map[Revision]struct{}{{10, 0}: {}}},
		// Compact at rev 12: gen0 removed, gen1 trimmed, keeps {12,0}
		{12, 2, map[Revision]struct{}{{12, 0}: {}}},
		// Compact at rev 14: tombstone of gen1. gen0 removed, gen1 trimmed to [{14,0}]. 2 gens.
		{14, 2, map[Revision]struct{}{{14, 0}: {}}},
		// Compact at rev 15: gen0 and gen1 both removed (tombstones < 15). Only gen2 remains. 1 gen.
		{15, 1, map[Revision]struct{}{}},
		// Compact at rev 16: only gen2, keeps {16,0}
		{16, 1, map[Revision]struct{}{{16, 0}: {}}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("compact(%d)", tt.compact), func(t *testing.T) {
			ki := newTest_KeyIndex()
			avail := make(map[Revision]struct{})
			if err := ki.compact(tt.compact, avail); err != nil {
				t.Fatalf("compact(%d): unexpected error: %v", tt.compact, err)
			}
			if len(ki.generations) != tt.wantGens {
				t.Errorf("compact(%d): generations = %d, want %d", tt.compact, len(ki.generations), tt.wantGens)
				for i, g := range ki.generations {
					t.Logf("  gen[%d]: created=%v revs=%v", i, g.created, g.revs)
				}
			}
			if !reflect.DeepEqual(avail, tt.wantAvail) {
				t.Errorf("compact(%d): avail = %v, want %v", tt.compact, avail, tt.wantAvail)
			}
		})
	}
}

func Test_KeyIndexCompactBeyondLastRevision(t *testing.T) {
	ki := &keyIndex{key: []byte("foo")}
	ki.put(1, 0)
	ki.put(2, 0)

	avail := make(map[Revision]struct{})
	ki.compact(100, avail)

	// Should keep only {2,0} — the latest revision
	wantAvail := map[Revision]struct{}{{2, 0}: {}}
	if !reflect.DeepEqual(avail, wantAvail) {
		t.Errorf("avail = %v, want %v", avail, wantAvail)
	}
	if len(ki.generations[0].revs) != 1 {
		t.Errorf("revs = %v, want [{2,0}]", ki.generations[0].revs)
	}
}

// ==================== keyIndex.keep tests ====================

func Test_KeyIndexKeepDoesNotMutate(t *testing.T) {
	ki := newTest_KeyIndex()
	clone := cloneKeyIndex(ki)

	avail := make(map[Revision]struct{})
	ki.keep(6, avail)

	if !ki.equal(clone) {
		t.Error("keep() mutated the keyIndex")
	}
	if _, ok := avail[Revision{6, 0}]; !ok {
		t.Error("keep(6) should have {6,0} in available")
	}
}

func Test_KeyIndexKeepExcludesTombstone(t *testing.T) {
	ki := newTest_KeyIndex()
	avail := make(map[Revision]struct{})
	ki.keep(8, avail) // rev 8 is a tombstone

	// Tombstone should NOT be in avail for keep()
	if _, ok := avail[Revision{8, 0}]; ok {
		t.Error("keep(8) should exclude tombstone revision from available")
	}
}

// ==================== keyIndex.restoreTombstone tests ====================

func Test_KeyIndexRestoreTombstone(t *testing.T) {
	ki := &keyIndex{key: []byte("foo")}
	if err := ki.restoreTombstone(16, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// get should return not found at any revision
	for rev := int64(16); rev <= 20; rev++ {
		_, _, _, err := ki.get(rev)
		if err == nil {
			t.Errorf("get(%d) should error after restoreTombstone", rev)
		}
	}

	// Should be able to put new revisions after restored tombstone
	ki.put(17, 0)
	ki.put(18, 0)
	_, mod, ver, err := ki.get(18)
	if err != nil {
		t.Fatalf("get(18): %v", err)
	}
	if mod != (Revision{18, 0}) || ver != 2 {
		t.Errorf("get(18) = mod:%v ver:%d, want mod:{18,0} ver:2", mod, ver)
	}
}

// ==================== keyIndex.equal tests ====================

func Test_KeyIndexEqual(t *testing.T) {
	a := newTest_KeyIndex()
	b := newTest_KeyIndex()
	if !a.equal(b) {
		t.Error("identical keyIndexes should be equal")
	}

	b.put(20, 0)
	if a.equal(b) {
		t.Error("different keyIndexes should not be equal")
	}
}

// ==================== helpers ====================

func cloneKeyIndex(ki *keyIndex) *keyIndex {
	c := &keyIndex{
		key:    make([]byte, len(ki.key)),
		modRev: ki.modRev,
	}
	copy(c.key, ki.key)
	for _, g := range ki.generations {
		ng := generation{
			version: g.version,
			created: g.created,
		}
		ng.revs = make([]Revision, len(g.revs))
		copy(ng.revs, g.revs)
		c.generations = append(c.generations, ng)
	}
	return c
}
