package kv

import (
	"encoding/binary"
	"fmt"
)

var (
	ErrEncodingFailed  = fmt.Errorf("codec error: encoding failed")
	ErrBufTooSmall     = fmt.Errorf("codec error: buffer too small")
	ErrSliceCopyFailed = fmt.Errorf("codec error: slice copy failed")
)

// NOTE: encoding/binary only allows putting uint64 into bytes
// so we need to constantly cast between uint64 <-> int64
// but its fine cuz no allocations happen, we just mix the bit interpretation of the bytes

func EncodeMeta(meta Meta) ([]byte, error) {
	// CreateRev int64  8
	// ModRev    int64  8
	// Version   int64  8
	// Tombstone bool    1
	buf := make([]byte, 8+8+8+1)
	binary.BigEndian.PutUint64(buf[0:8], uint64(meta.CreateRev))
	binary.BigEndian.PutUint64(buf[8:16], uint64(meta.ModRev))
	binary.BigEndian.PutUint64(buf[16:24], uint64(meta.Version))

	if meta.Tombstone {
		buf[24] = 1
	} else {
		buf[24] = 0
	}

	return buf, nil
}

func DecodeMeta(src []byte) (Meta, error) {
	if len(src) < 8+8+8+1 {
		return Meta{}, ErrBufTooSmall
	}

	meta := Meta{
		CreateRev: int64(binary.BigEndian.Uint64(src[0:8])),
		ModRev:    int64(binary.BigEndian.Uint64(src[8:16])),
		Version:   int64(binary.BigEndian.Uint64(src[16:24])),
		Tombstone: src[24] == 1,
	}

	return meta, nil
}

func EncodeCompositeKey(key CompositeKey) ([]byte, error) {
	// Key []byte 		4 (len) + len(Key)
	// Revision uint64 	8
	buf := make([]byte, 4+len(key.Key)+8+8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(key.Key)))
	copy(buf[4:4+len(key.Key)], key.Key)
	if n := copy(buf[4:4+len(key.Key)], key.Key); n != len(key.Key) {
		return nil, ErrSliceCopyFailed
	}
	binary.BigEndian.PutUint64(buf[4+len(key.Key):], uint64(key.Rev.Main))
	binary.BigEndian.PutUint64(buf[4+len(key.Key)+8:], uint64(key.Rev.Sub))
	return buf, nil
}

func DecodeCompositeKey(src []byte) (CompositeKey, error) {
	if len(src) < 4 {
		return CompositeKey{}, ErrBufTooSmall
	}
	keyLen := int(binary.BigEndian.Uint32(src[0:4]))
	// already check for []byte PLUS revision to save another check
	if len(src) < 4+keyLen+8+8 {
		return CompositeKey{}, ErrBufTooSmall
	}

	var k CompositeKey
	k.Key = make([]byte, keyLen)
	if n := copy(k.Key, src[4:4+keyLen]); n != keyLen {
		return CompositeKey{}, ErrSliceCopyFailed
	}

	k.Rev.Main = int64(binary.BigEndian.Uint64(src[4+keyLen:]))
	k.Rev.Sub = int64(binary.BigEndian.Uint64(src[4+keyLen+8:]))
	return k, nil
}

func EncodeRevision(rev Revision) ([]byte, error) {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf, uint64(rev.Main))
	binary.BigEndian.PutUint64(buf, uint64(rev.Sub))
	return buf, nil
}

func DecodeRevision(src []byte) (rev Revision, err error) {
	if len(src) < 16 {
		err = ErrBufTooSmall
		return
	}
	rev.Main = int64(binary.BigEndian.Uint64(src[0:8]))
	rev.Sub = int64(binary.BigEndian.Uint64(src[8:16]))
	return
}
