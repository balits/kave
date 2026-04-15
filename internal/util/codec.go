package util

import (
	"encoding/binary"
	"fmt"
)

var (
	errDecodingFailed = fmt.Errorf("codec error: entry decoding failed")
)

func EncodeUint64(v uint64) (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return
}

func DecodeUint64(src []byte) (out uint64, err error) {
	if len(src) < 8 {
		err = fmt.Errorf("%v: buffer too small", errDecodingFailed)
		return
	}
	out = binary.BigEndian.Uint64(src)
	return
}

func DecodeInt64(src []byte) (out int64, err error) {
	var u uint64
	u, err = DecodeUint64(src)
	if err != nil {
		return
	}
	return int64(u), nil
}
