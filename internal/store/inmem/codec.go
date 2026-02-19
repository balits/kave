package inmem

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/google/btree"
)

// Encode is responsible for writing the snapshot
// to the given writer. It uses a simple, gob backed
// protocol: first the length or size of tree is encoded,
// then in order, the key-value elements of the tree are encoded
func Encode(w io.Writer, snapshot Snapshot) error {
	var (
		enc = gob.NewEncoder(w)
		err error
	)

	if err = enc.Encode(snapshot.Tree.Len()); err != nil {
		return err
	}

	snapshot.Tree.Ascend(func(item btree.Item) bool {
		err = enc.Encode(item.(KVBtreeItem))
		return err == nil
	})

	if err != nil {
		return fmt.Errorf("could not ascend btree successfuly")
	}

	return nil
}

// Decode is responsible for building up the BTree from
// the given reader. It shares the protocol with [Encode]
// first the length or size of tree is decoded,
// followed by that many items, which are then inserted
// to the tree
func Decode(r io.Reader) (*btree.BTree, error) {
	var (
		count int
		err   error
		tree  = btree.New(BtreeDegreeDefault)
		dec   = gob.NewDecoder(r)
	)

	if err = dec.Decode(&count); err != nil {
		return nil, err
	}

	for range count {
		var item KVBtreeItem

		if err = dec.Decode(&item); err != nil {
			return nil, err
		}

		tree.ReplaceOrInsert(item)
	}

	return tree, nil
}
