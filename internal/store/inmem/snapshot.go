package inmem

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/google/btree"
	"github.com/hashicorp/raft"
)

// Snapshot is a snapshot of the data at a given time, used to support log compaction and restore the FSM to a desired state.
// It is returned FSM.Snapshot() which itself shouldn't do heavy IO work.
// No mutex needed since snapshots are immutable
type Snapshot struct {
	Tree *btree.BTree
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s Snapshot) Persist(sink raft.SnapshotSink) error {
	if err := Encode(sink, s); err != nil {
		err2 := sink.Cancel()
		if err2 != nil {
			return errors.Join(err, err2)
		}
		return err
	}
	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (s Snapshot) Release() {
	//let gc clean it up later automatically
	s.Tree = nil
}

// Equals helps us to compare two snapshots
// only used in tests
func (s Snapshot) Equals(that *btree.BTree) bool {
	if !testing.Testing() {
		panic("InmemSnapshot.Equals only works in test environments!")
	}

	fmt.Println("tree1 len", s.Tree.Len())
	fmt.Println("tree2 len", that.Len())

	if s.Tree.Len() != that.Len() {
		fmt.Printf("tree sizes differ")
		return false
	}

	var (
		items1 = make([]KVBtreeItem, s.Tree.Len())
		items2 = make([]KVBtreeItem, that.Len())
	)

	s.Tree.Ascend(func(item btree.Item) bool {
		fmt.Printf("items1: %s - %s \n", string(item.(KVBtreeItem).Key), string(item.(KVBtreeItem).Value))
		items1 = append(items1, item.(KVBtreeItem))
		return true
	})

	that.Ascend(func(item btree.Item) bool {
		items2 = append(items2, item.(KVBtreeItem))
		return true
	})

	for i := range items1 {
		fmt.Printf(
			"%s <-> %s\n",
			fmt.Sprintf("%s-%s", items1[i].Key, items1[i].Value),
			fmt.Sprintf("%s-%s", items2[i].Key, items2[i].Value),
		)

		keys := bytes.Equal(items1[i].Key, items2[i].Key)
		vals := bytes.Equal(items1[i].Value, items2[i].Value)
		if keys && vals {
			return false
		}
	}

	return true
}
