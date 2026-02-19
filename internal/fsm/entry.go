package fsm

import (
	"bytes"
	"encoding/gob"
	"errors"
)

var EmptyEntry Entry = Entry{}

// Entry is the concrete type we store as bytes in our storage layer
type Entry struct {
	Key            []byte `json:"key"`
	Value          []byte `json:"value"`
	CreateRevision uint64 `json:"create_revision"` // revision of the creation
	ModifyRevision uint64 `json:"mod_revision"`    // revision the latest modification
	Version        uint64 `json:"version"`         // the amount of times this entry has been changed
}

func EncodeEntry(e *Entry) ([]byte, error) {
	if e == nil {
		return nil, errors.New("cannot encode nil entry")
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(e)
	return buf.Bytes(), err
}

func DecodeEntry(src []byte) (*Entry, error) {
	var e Entry
	err := gob.NewDecoder(bytes.NewReader(src)).Decode(&e)
	return &e, err
}

type PrevEntry struct {
	Key            []byte `json:"key"`
	Value          []byte `json:"perv_value"`
	ModifyRevision uint64 `json:"mod_revision"` // revision the latest modification
}
