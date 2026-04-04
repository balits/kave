package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Entry a valódi tipus amit az adatbázisban tárolunk
type Entry struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	CreateRev int64  `json:"create_revision"`
	ModRev    int64  `json:"mod_revision"`
	Version   int64  `json:"version"`
	LeaseID   int64  `json:"lease_id,omitempty"`
}

func (e Entry) Tombstone() bool {
	return len(e.Key) != 0 && len(e.Value) == 0
}

func (e Entry) String() string {
	return fmt.Sprintf(
		"Entry{Key: %s, Value: %s, CreateRev: %d, ModRev: %d, Version: %d, LeaseID: %d}",
		string(e.Key),
		string(e.Value),
		e.CreateRev,
		e.ModRev,
		e.Version,
		e.LeaseID,
	)
}

func EncodeKvEntry(e *Entry) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("codec error: failed to encode nil entry")
	}
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

func DecodeEntry(data []byte) (*Entry, error) {
	if data == nil {
		return nil, errors.New("codec error: nil buffer recieved")
	}
	e := new(Entry)
	offset := 0

	if len(data) < 4 {
		return e, errors.New("codec error: decoding failed: data too short for key length")
	}
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+keyLen {
		return e, errors.New("codec error: decoding failed: data too short for key")
	}
	e.Key = make([]byte, keyLen)
	copy(e.Key, data[offset:offset+keyLen])
	offset += keyLen

	if len(data) < offset+4 {
		return e, errors.New("codec error: decoding failed: data too short for value length")
	}
	valLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if len(data) < offset+valLen {
		return e, errors.New("codec error: decoding failed: data too short for value")
	}
	e.Value = make([]byte, valLen)
	copy(e.Value, data[offset:offset+valLen])
	offset += valLen

	if len(data) < offset+32 {
		return e, errors.New("codec error: decoding failed: data too short for fixed fields")
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
