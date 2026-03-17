package types

import (
	"encoding/binary"
	"fmt"
)

var (
	errEncodingFailed = fmt.Errorf("codec error: entry encoding failed")
	errDecodingFailed = fmt.Errorf("codec error: entry decoding failed")
)

func EncodeKvEntry(e KvEntry) ([]byte, error) {
	totalLen := 4 + len(e.Key) + 4 + len(e.Value) + 8 + 8 + 8 + 8
	buf := make([]byte, totalLen)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Key)))
	offset += 4
	copy(buf[offset:], e.Key)
	offset += len(e.Key)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Value)))
	offset += 4
	copy(buf[offset:], e.Value)
	offset += len(e.Value)

	binary.BigEndian.PutUint64(buf[offset:], uint64(e.CreateRev))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], uint64(e.ModRev))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], uint64(e.Version))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], uint64(e.LeaseID))
	return buf, nil
}

func DecodeKvEntry(data []byte) (KvEntry, error) {
	var e KvEntry
	offset := 0

	if len(data) < 4 {
		return e, fmt.Errorf("%w: data too short for key length", errDecodingFailed)
	}
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+keyLen {
		return e, fmt.Errorf("%w: data too short for key", errDecodingFailed)
	}
	e.Key = make([]byte, keyLen)
	copy(e.Key, data[offset:offset+keyLen])
	offset += keyLen

	if len(data) < offset+4 {
		return e, fmt.Errorf("%w: data too short for value length", errDecodingFailed)
	}
	valLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+valLen {
		return e, fmt.Errorf("%w: data too short for value", errDecodingFailed)
	}
	e.Value = make([]byte, valLen)
	copy(e.Value, data[offset:offset+valLen])
	offset += valLen

	if len(data) < offset+32 {
		return e, fmt.Errorf("%w: data too short for fixed fields", errDecodingFailed)
	}
	e.CreateRev = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	e.ModRev = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	e.Version = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	e.LeaseID = int64(binary.BigEndian.Uint64(data[offset:]))

	return e, nil
}

func EncodeUint64(v uint64) (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return
}

func DecodeUint64(src []byte) (out uint64, err error) {
	if len(src) < 8 {
		err = fmt.Errorf("%v: buffer too small", errDecodingFailed)
	}
	out = binary.BigEndian.Uint64(src)
	return
}
