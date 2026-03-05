package inmem

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
)

const (
	BtreeDegreeDefault  = 32
	BtreeDegreeBatching = 16
)

type KVBtreeItem struct {
	Key   []byte
	Value []byte
}

func (i KVBtreeItem) Less(than btree.Item) bool {
	return bytes.Compare(i.Key, than.(KVBtreeItem).Key) < 0
}

func (i KVBtreeItem) String() string {
	return fmt.Sprintf("<%s, %s>", i.Key, i.Value)
}
