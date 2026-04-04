package kv

import "fmt"

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

func (r *Revision) String() string {
	return fmt.Sprintf("(%d,%d)", r.Main, r.Sub)
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

func NewRevBytes() []byte {
	return make([]byte, revBytesLen, markedRevBytesLen)
}

func EncodeRevisionAsBucketKey(rev Revision, buf []byte) []byte {
	return EncodeKvBucketKey(KvBucketKey{Revision: rev}, buf)
}

func DecodeRevision(b []byte) Revision {
	return DecodeKvBucketKey(b).Revision
}
