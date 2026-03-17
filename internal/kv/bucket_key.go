package kv

import (
	"encoding/binary"
	"fmt"
)

// KvBucketKey a valódi kulcs amit a backendben használunk: egy revízió kiegészítve a Tombstone kapcsolóval
type KvBucketKey struct {
	Revision
	Tombstone bool
}

func NewKvBucketKey(main, sub int64, isTombstone bool) KvBucketKey {
	return KvBucketKey{
		Revision:  Revision{Main: main, Sub: sub},
		Tombstone: isTombstone,
	}
}

func EncodeKvBucketKey(bk KvBucketKey, buf []byte) []byte {
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

func DecodeKvBucketKey(src []byte) KvBucketKey {
	if len(src) != revBytesLen && len(src) != markedRevBytesLen {
		panic(fmt.Errorf("invalid revision key length: %d", len(src)))
	}
	return KvBucketKey{
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
