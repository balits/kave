package kv

import (
	"encoding/binary"
	"fmt"
)

const (
	revBytesLen       = 16 // 8 (main) + 8 (sub)
	markedRevBytesLen = 17 // + 1 tombstone marker
	markBytePosition  = 16
	markTombstone     = byte('T')
)

type Revision struct {
	Main int64
	Sub  int64
}

func (r Revision) AddMain(delta int64) Revision {
	return Revision{
		Main: r.Main + delta,
		Sub:  r.Sub,
	}
}

func (r Revision) AddSub(delta int64) Revision {
	return Revision{
		Main: r.Main,
		Sub:  r.Sub + delta,
	}
}

func (r Revision) GreaterThan(b Revision) bool {
	if r.Main > b.Main {
		return true
	}
	if r.Main < b.Main {
		return false
	}
	return r.Sub > b.Sub
}

type BucketKey struct {
	Revision
	Tombstone bool
}

func NewBucketKey(main, sub int64, isTombstone bool) BucketKey {
	return BucketKey{
		Revision:  Revision{Main: main, Sub: sub},
		Tombstone: isTombstone,
	}
}

func NewRevBytes() []byte {
	return make([]byte, revBytesLen, markedRevBytesLen)
}

func RevToBytes(rev Revision, buf []byte) []byte {
	return BucketKeyToBytes(BucketKey{Revision: rev}, buf)
}

func BytesToRev(b []byte) Revision {
	return BytesToBucketKey(b).Revision
}

func BucketKeyToBytes(bk BucketKey, buf []byte) []byte {
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

func BytesToBucketKey(b []byte) BucketKey {
	if len(b) != revBytesLen && len(b) != markedRevBytesLen {
		panic(fmt.Sprintf("invalid revision key length: %d", len(b)))
	}
	return BucketKey{
		Revision: Revision{
			Main: int64(binary.BigEndian.Uint64(b[0:8])),
			Sub:  int64(binary.BigEndian.Uint64(b[8:16])),
		},
		Tombstone: len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone,
	}
}

func IsTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
