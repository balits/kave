package fsm

import (
	"bytes"
	"errors"

	"github.com/balits/kave/internal/common"
)

var (
	ErrInvalidCompareTarget   error = errors.New("invalid compare target")
	ErrInvalidCompareOperator error = errors.New("invalid compare operator")
)

type Condition struct {
	TargetUnion TargetFieldUnion `json:"target_union"`
	Key         []byte           `json:"key"`
	Operator    CompareOperator  `json:"operator"`
	Target      CompareTarget    `json:"target"`
}

func (c *Condition) Eval(targetEntry *common.Entry) (result bool) {
	var (
		op1 TargetFieldUnion
	)

	switch c.Target {
	case TargetVersion:
		op1 = TargetFieldUnion{Version: &targetEntry.Version}
	case TargetCreate:
		op1 = TargetFieldUnion{CreateRevision: &targetEntry.CreateRevision}
	case TargetMod:
		op1 = TargetFieldUnion{ModRevision: &targetEntry.ModRevision}
	case TargetValue:
		op1 = TargetFieldUnion{Value: targetEntry.Value}
	}

	switch c.Operator {
	case CompareEqual:
		result = eq(op1, c.TargetUnion, c.Target)
	case CompareGreater:
		result = gt(op1, c.TargetUnion, c.Target)
	case CompareLess:
		result = lt(op1, c.TargetUnion, c.Target)
	case CompareNotEqual:
		result = neq(op1, c.TargetUnion, c.Target)
	}
	return
}

func Value(key string, value []byte, op CompareOperator) Condition {
	return Condition{
		Key:         []byte(key),
		Operator:    op,
		Target:      TargetValue,
		TargetUnion: TargetFieldUnion{Value: value},
	}
}

func CreateRevision(key string, rev uint64, op CompareOperator) Condition {
	return Condition{
		Key:         []byte(key),
		Operator:    op,
		Target:      TargetCreate,
		TargetUnion: TargetFieldUnion{CreateRevision: &rev},
	}
}

func ModRevision(key string, rev uint64, op CompareOperator) Condition {
	return Condition{
		Key:         []byte(key),
		Operator:    op,
		Target:      TargetMod,
		TargetUnion: TargetFieldUnion{ModRevision: &rev},
	}
}

func Version(key string, version uint64, op CompareOperator) Condition {
	return Condition{
		Key:         []byte(key),
		Operator:    op,
		Target:      TargetVersion,
		TargetUnion: TargetFieldUnion{Version: &version},
	}
}

type CompareOperator string

const (
	CompareEqual    CompareOperator = "="
	CompareGreater  CompareOperator = ">"
	CompareLess     CompareOperator = "<"
	CompareNotEqual CompareOperator = "!="
)

type CompareTarget string

const (
	TargetVersion CompareTarget = "VERSION"
	TargetCreate  CompareTarget = "CREATE"
	TargetMod     CompareTarget = "MOD"
	TargetValue   CompareTarget = "VALUE"
)

type TargetFieldUnion struct {
	Value          []byte  `json:"value,omitempty"`
	CreateRevision *uint64 `json:"create_revision,omitempty"`
	ModRevision    *uint64 `json:"mod_revision,omitempty"`
	Version        *uint64 `json:"version,omitempty"`
}

const unknownTargetMsg = "unknown compare target"

func eq(op1, op2 TargetFieldUnion, target CompareTarget) bool {
	switch target {
	case TargetVersion:
		return *op1.Version == *op2.Version
	case TargetCreate:
		return *op1.CreateRevision == *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision == *op2.ModRevision
	case TargetValue:
		return bytes.Equal(op1.Value, op2.Value)
	default:
		panic(unknownTargetMsg)
	}
}

func gt(op1, op2 TargetFieldUnion, target CompareTarget) bool {
	switch target {
	case TargetVersion:
		return *op1.Version > *op2.Version
	case TargetCreate:
		return *op1.CreateRevision > *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision > *op2.ModRevision
	case TargetValue:
		return bytes.Compare(op1.Value, op2.Value) > 0
	default:
		panic(unknownTargetMsg)
	}
}

func lt(op1, op2 TargetFieldUnion, target CompareTarget) bool {
	switch target {
	case TargetVersion:
		return *op1.Version < *op2.Version
	case TargetCreate:
		return *op1.CreateRevision < *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision < *op2.ModRevision
	case TargetValue:
		return bytes.Compare(op1.Value, op2.Value) < 0
	default:
		panic(unknownTargetMsg)
	}
}

func neq(op1, op2 TargetFieldUnion, target CompareTarget) bool {
	switch target {
	case TargetVersion:
		return *op1.Version != *op2.Version
	case TargetCreate:
		return *op1.CreateRevision != *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision != *op2.ModRevision
	case TargetValue:
		return !bytes.Equal(op1.Value, op2.Value)
	default:
		panic(unknownTargetMsg)
	}
}

// ValidateCompare should check that the targetUnion field is well formed, i.e. that the field corresponding
// to the target field is set and all other fields are nil.The union fields value should also match the compare target
func ValidateCompare(c *Condition) error {
	switch c.Target {
	case TargetVersion:
		if c.TargetUnion.Version == nil || c.TargetUnion.CreateRevision != nil || c.TargetUnion.ModRevision != nil || c.TargetUnion.Value != nil {
			errors.New("invalid Cmp: target is CompareVersion but targetUnion does not have version set or has other fields set")
		}
	case TargetCreate:
		if c.TargetUnion.CreateRevision == nil || c.TargetUnion.Version != nil || c.TargetUnion.ModRevision != nil || c.TargetUnion.Value != nil {
			errors.New("invalid Cmp: target is CompareCreate but targetUnion does not have create_revision set or has other fields set")
		}
	case TargetMod:
		if c.TargetUnion.ModRevision == nil || c.TargetUnion.Version != nil || c.TargetUnion.CreateRevision != nil || c.TargetUnion.Value != nil {
			errors.New("invalid Cmp: target is CompareMod but targetUnion does not have mod_revision set or has other fields set")
		}
	case TargetValue:
		if c.TargetUnion.Value == nil || c.TargetUnion.Version != nil || c.TargetUnion.CreateRevision != nil || c.TargetUnion.ModRevision != nil {
			errors.New("invalid Cmp: target is CompareValue but targetUnion does not have value set or has other fields set")
		}
	default:
		errors.New(unknownTargetMsg)
	}

	return nil
}
