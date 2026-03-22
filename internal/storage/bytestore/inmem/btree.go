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

type Item struct {
	Key   []byte
	Value []byte
}

func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i.Key, than.(Item).Key) < 0
}

func (i Item) String() string {
	return fmt.Sprintf("<%s, %s>", i.Key, i.Value)
}
