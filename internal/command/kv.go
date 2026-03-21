package command

import (
	"errors"
	"fmt"

	"github.com/balits/kave/internal/types/api"
)

// A RangeCmd alparancs a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva,
// és ha End nil akkor csak a Key-t olvassuk
type RangeCmd = api.KvRangeRequest

// A RangeResult egy Range művelet eredménye
type RangeResult = api.KvRangeResponseNoHeader

// A PutCmd alparancs a PUT művelethez szükséges adatokat tartalmazza
type PutCmd = api.KvPutRequest

type PutResult = api.KvPutResponseNoHeader

// A DeleteCmd alparancs a DELETE művelethez szükséges adatokat tartalmazza
type DeleteCmd = api.KvDeleteRequest

type DeleteResult = api.KvDeleteResponseNoHeader

type TxnCmd struct {
	Comparisons []Comparison `json:"comparisons"`
	Success     []TxnOp      `json:"success"`
	Failure     []TxnOp      `json:"failure"`
}

func (c *TxnCmd) Check() error {
	for _, cmp := range c.Comparisons {
		if err := cmp.Check(); err != nil {
			return fmt.Errorf("invalid comparison: %w", err)
		}
	}
	for _, op := range c.Success {
		if err := op.Check(); err != nil {
			return fmt.Errorf("invalid success operation: %w", err)
		}
	}
	for _, op := range c.Failure {
		if err := op.Check(); err != nil {
			return fmt.Errorf("invalid failure operation: %w", err)
		}
	}
	return nil
}

type TxnResult struct {
	// A Success jelzi, hogy melyik ág lett választva (true = összehasonlítások sikeresek voltak)
	Success bool `json:"success"`

	// A Results egy bejegyzést tartalmaz az összes operációhoz a választott ágban, sorrendben
	Results []TxnOpResult `json:"results"`
}

type TxnOpType string

const (
	TxnOpPut    TxnOpType = "PUT"
	TxnOpDelete TxnOpType = "DEL"
	TxnOpRange  TxnOpType = "RANGE"
)

// TxnOp egy művelet egy TxnCommand-ben, lehet PUT, DELETE vagy RANGE
type TxnOp struct {
	Type   TxnOpType  `json:"type"`
	Put    *PutCmd    `json:"put,omitempty"`
	Delete *DeleteCmd `json:"delete,omitempty"`
	Range  *RangeCmd  `json:"range,omitempty"`
}

func (op *TxnOp) Check() error {
	switch op.Type {
	case TxnOpPut:
		if op.Put == nil {
			return errors.New("put command is required for put operation")
		}
		return op.Put.Check()
	case TxnOpDelete:
		if op.Delete == nil {
			return errors.New("delete command is required for delete operation")
		}
		return op.Delete.Check()
	case TxnOpRange:
		if op.Range == nil {
			return errors.New("range command is required for range operation")
		}
		return op.Range.Check()
	default:
		return fmt.Errorf("invalid operation type: %s", op.Type)
	}
}

// A TxnOpResult unió egy tranzakcióban szereplő műveleti eredményt tartalmazza
// Pontosan egy mező nem nil, amely az operáció típusához illeszkedik
type TxnOpResult struct {
	Put    *PutResult    `json:"put,omitempty"`
	Delete *DeleteResult `json:"delete,omitempty"`
	Range  *RangeResult  `json:"range,omitempty"`
}
