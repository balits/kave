package kv

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

func NewRevBytes() []byte {
	return make([]byte, revBytesLen, markedRevBytesLen)
}

func EncodeRevision(rev Revision, buf []byte) []byte {
	return EncodeKVBucketKey(KVBucketKey{Revision: rev}, buf)
}

func DecodeRevision(b []byte) Revision {
	return DecodeKVBucketKey(b).Revision
}
