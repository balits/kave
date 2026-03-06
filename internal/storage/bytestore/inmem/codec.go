package inmem

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/balits/kave/internal/storage"
	"github.com/google/btree"
)

type codecError struct {
	err   error
	field string
}

func (c codecError) Error() string {
	return fmt.Sprintf("codec error (while working on %s): %s ", c.field, c.err.Error())
}

// Encode is responsible for writing the snapshot
// to the given writer. It uses a simple, gob backed protocol:
// first the number of buckets are encoded, then for each bucket, its name and size is encoded,
// then in order, the key-value elements of the tree are encoded
func Encode(w io.Writer, store *InmemStore) error {
	enc := gob.NewEncoder(w)

	if err := enc.Encode(len(store.buckets)); err != nil {
		return codecError{err: err, field: "buckets count"}
	}

	// for each bucket encode:
	// - name
	// - len of items
	// - items
	for bucket, tree := range store.buckets {
		if err := enc.Encode(bucket); err != nil {
			return codecError{err: err, field: "bucket name"}
		}

		if err := encodeTree(enc, tree); err != nil {
			return err
		}
	}

	return nil
}

// Decode is responsible for reading the snapshot from the given reader and reconstructing the in-memory tree
// It uses the same protocol as Encode, first it reads the number of buckets, then for each bucket, its name is read,
// then in order, the key-value elements of the tree is read
func Decode(r io.Reader) (map[storage.Bucket]*btree.BTree, error) {
	var (
		dec         = gob.NewDecoder(r)
		buckets     = make(map[storage.Bucket]*btree.BTree)
		bucketCount int
	)

	if err := dec.Decode(&bucketCount); err != nil {
		return nil, codecError{err: err, field: "buckets count"}
	}

	for i := 0; i < bucketCount; i++ {
		var bucket storage.Bucket
		if err := dec.Decode(&bucket); err != nil {
			return nil, codecError{err: err, field: "bucket name"}
		}

		tree, err := decodeTree(dec)
		if err != nil {
			return nil, err
		}

		buckets[bucket] = tree
	}

	return buckets, nil
}

func encodeTree(enc *gob.Encoder, tree *btree.BTree) *codecError {
	var err error
	if err = enc.Encode(tree.Len()); err != nil {
		return &codecError{err: err, field: "btree item count"}
	}

	tree.Ascend(func(item btree.Item) bool {
		err = enc.Encode(item.(KVBtreeItem))
		return err == nil
	})

	if err != nil {
		return &codecError{err: fmt.Errorf("could not ascend btree: %w", err), field: "btree items"}
	}

	return nil
}

func decodeTree(dec *gob.Decoder) (*btree.BTree, *codecError) {
	var (
		count int
		err   error
		tree  = btree.New(BtreeDegreeDefault)
	)

	if err = dec.Decode(&count); err != nil {
		return nil, &codecError{err: err, field: "btree item count"}
	}

	for range count {
		var item KVBtreeItem
		if err = dec.Decode(&item); err != nil {
			return nil, &codecError{err: err, field: "btree item"}
		}

		tree.ReplaceOrInsert(item)
	}

	return tree, nil
}
