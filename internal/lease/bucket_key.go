package lease

import (
	"encoding/binary"
)

type LeaseBucketKey int64

func EncodeLeaseBucketKey(bk LeaseBucketKey) []byte {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, uint64(bk))
	return buff
}

func DecodeLeaseBucketKey(src []byte) LeaseBucketKey {
	if len(src) < 8 {
		panic("coded error: decode lease bucket key failed: buffer is less than 8 bytes long")
	}
	i := binary.BigEndian.Uint64(src[:8])
	return LeaseBucketKey(i)
}
