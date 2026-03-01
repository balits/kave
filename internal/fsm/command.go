package fsm

import (
	"bytes"
	"encoding/gob"

	"github.com/balits/kave/internal/storage"
)

type CommandType int

const (
	CmdSet CommandType = iota
	CmdDelete
	CmdTxn
)

func (t CommandType) String() string {
	switch t {
	case CmdSet:
		return "SET"
	case CmdDelete:
		return "DELETE"
	case CmdTxn:
		return "TXN"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	Type   CommandType
	Bucket storage.Bucket
	Key    []byte

	// for SET operations
	Value []byte

	// for TXN operations
	Txn *Txn
}

func EncodeCommand(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(cmd)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeCommand(src []byte) (Command, error) {
	var cmd Command
	buf := bytes.NewReader(src)
	err := gob.NewDecoder(buf).Decode(&cmd)
	if err != nil {
		return Command{}, err
	}
	return cmd, nil
}
