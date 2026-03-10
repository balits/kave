package kv

import (
	"encoding/binary"
	"fmt"
)

type KVBucketKey struct {
	Revision
	Tombstone bool
}

func NewKVBucketKey(main, sub int64, isTombstone bool) KVBucketKey {
	return KVBucketKey{
		Revision:  Revision{Main: main, Sub: sub},
		Tombstone: isTombstone,
	}
}

func EncodeKVBucketKey(bk KVBucketKey, buf []byte) []byte {
	binary.BigEndian.PutUint64(buf[0:8], uint64(bk.Main))
	binary.BigEndian.PutUint64(buf[8:16], uint64(bk.Sub))
	if bk.Tombstone {
		if len(buf) == revBytesLen {
			buf = append(buf, markTombstone)
		} else if len(buf) >= markedRevBytesLen {
			buf[markBytePosition] = markTombstone
		}
	}
	return buf
}

func DecodeKVBucketKey(src []byte) KVBucketKey {
	if len(src) != revBytesLen && len(src) != markedRevBytesLen {
		panic(fmt.Errorf("invalid revision key length: %d", len(src)))
	}
	return KVBucketKey{
		Revision: Revision{
			Main: int64(binary.BigEndian.Uint64(src[0:8])),
			Sub:  int64(binary.BigEndian.Uint64(src[8:16])),
		},
		Tombstone: len(src) == markedRevBytesLen && src[markBytePosition] == markTombstone,
	}
}

func IsTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
