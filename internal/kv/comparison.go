package kv

import (
	"bytes"
)

// TODO: no checks for union field

type Comparison struct {
	Key         []byte             `json:"key"`          // key of the value ot compare
	Operator    ComparisonOperator `json:"operator"`     // logical operator to apply
	Target      CompareTarget      `json:"target"`       // which field to compare against
	TargetUnion TargetFieldUnion   `json:"target_union"` // actual value of Target
}

// MatchZeroValue returns true if the comparison matches against a zero value (i.e. no metadata or value)
func (c *Comparison) MatchZeroValue() bool {
	return c.Eval(EmptyEntry)
}

func (c *Comparison) Eval(targetEntry Entry) (result bool) {
	var t TargetFieldUnion

	switch c.Target {
	case TargetVersion:
		t = TargetFieldUnion{Version: &targetEntry.Version}
	case TargetCreate:
		t = TargetFieldUnion{CreateRevision: &targetEntry.CreateRev}
	case TargetMod:
		t = TargetFieldUnion{ModRevision: &targetEntry.ModRev}
	case TargetValue:
		t = TargetFieldUnion{Value: targetEntry.Value}
	}

	return eval(c.Operator, c.Target, t, c.TargetUnion)
}

type ComparisonOperator string

const (
	OperatorEqual        ComparisonOperator = "="
	OperatorGreaterThan  ComparisonOperator = ">"
	OperatorGreaterEqual ComparisonOperator = ">="
	OperatorLessThan     ComparisonOperator = "<"
	OperatorLessEqual    ComparisonOperator = "<="
	OperatorNotEqual     ComparisonOperator = "!="
)

type CompareTarget string

const (
	TargetValue   CompareTarget = "VALUE"
	TargetCreate  CompareTarget = "CREATE"
	TargetMod     CompareTarget = "MOD"
	TargetVersion CompareTarget = "VERSION"
)

type TargetFieldUnion struct {
	Value          []byte  `json:"value,omitempty"`
	CreateRevision *int64 `json:"create_revision,omitempty"`
	ModRevision    *int64 `json:"mod_revision,omitempty"`
	Version        *int64 `json:"version,omitempty"`
}

const unknownTargetMsg = "unknown compare target"

func eval(op ComparisonOperator, target CompareTarget, a, b TargetFieldUnion) bool {
	switch op {
	case OperatorEqual:
		return eq(target, a, b)
	case OperatorGreaterThan:
		return gt(target, a, b)
	case OperatorGreaterEqual:
		return gt(target, a, b) || eq(target, a, b)
	case OperatorLessThan:
		return lt(target, a, b)
	case OperatorLessEqual:
		return lt(target, a, b) || eq(target, a, b)
	case OperatorNotEqual:
		return !eq(target, a, b)
	default:
		// panic(unknownOperatorMsg)
		return false // default to false instead of panicking
	}
}

func eq(target CompareTarget, op1, op2 TargetFieldUnion) bool {
	switch target {
	case TargetValue:
		return bytes.Equal(op1.Value, op2.Value)
	case TargetCreate:
		return *op1.CreateRevision == *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision == *op2.ModRevision
	case TargetVersion:
		return *op1.Version == *op2.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}

func gt(target CompareTarget, op1, op2 TargetFieldUnion) bool {
	switch target {
	case TargetValue:
		return bytes.Compare(op1.Value, op2.Value) > 0
	case TargetCreate:
		return *op1.CreateRevision > *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision > *op2.ModRevision
	case TargetVersion:
		return *op1.Version > *op2.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}

func lt(target CompareTarget, op1, op2 TargetFieldUnion) bool {
	switch target {
	case TargetValue:
		return bytes.Compare(op1.Value, op2.Value) < 0
	case TargetCreate:
		return *op1.CreateRevision < *op2.CreateRevision
	case TargetMod:
		return *op1.ModRevision < *op2.ModRevision
	case TargetVersion:
		return *op1.Version < *op2.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}
