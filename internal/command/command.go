package command

import (
	"bytes"
	"encoding/gob"
)

type CommandType uint8

const (
	CommandTypeGet CommandType = iota
	CommandTypeSet
	CommandTypeDelete
	CommandTypeBatch
	CommandTypeCompareAndSwap
)

func (t CommandType) String() string {
	switch t {
	case CommandTypeGet:
		return "GET"
	case CommandTypeSet:
		return "SET"
	case CommandTypeDelete:
		return "DELETE"
	case CommandTypeBatch:
		return "BATCH"
	case CommandTypeCompareAndSwap:
		return "CAS"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	Type CommandType
	Key  string

	// for SET operations
	Value []byte

	// for BATCH operations
	BatchOps []Command

	// for CAS operations
	ExpectedRevision *uint64
}

func Encode(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(cmd)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(src []byte) (Command, error) {
	var cmd Command
	buf := bytes.NewReader(src)
	err := gob.NewDecoder(buf).Decode(&cmd)
	if err != nil {
		return Command{}, err
	}
	return cmd, nil
}
