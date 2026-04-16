package api

import (
	"bytes"
	"fmt"

	"github.com/balits/kave/internal/kv"
)

// TODO: no checks for union field
type Comparison struct {
	Key         []byte             `json:"key"`          // key of the value ot compare
	Operator    ComparisonOperator `json:"operator"`     // logical operator to apply
	TargetField CompareTargetField `json:"target_field"` // which field to compare against
	TargetValue CompareTargetUnion `json:"target_value"` // actual value of Target
}

func (c *Comparison) Check() error {
	if len(c.Key) == 0 {
		return fmt.Errorf("comparison key is required")
	}
	switch c.Operator {
	case OperatorEqual, OperatorGreaterThan, OperatorGreaterEqual, OperatorLessThan, OperatorLessEqual, OperatorNotEqual:
		// ok
	default:
		return fmt.Errorf("invalid comparison operator: %s", c.Operator)
	}
	switch c.TargetField {
	case FieldValue, FieldCreateRev, FieldMod, FieldVersion:
		// ok
	default:
		return fmt.Errorf("invalid comparison target field: %s", c.TargetField)
	}
	return nil
}

func (c *Comparison) Eval(target *kv.Entry) (result bool) {
	if target == nil {
		target = &kv.Entry{}
	}

	var t CompareTargetUnion

	switch c.TargetField {
	case FieldVersion:
		t = CompareTargetUnion{Version: target.Version}
	case FieldCreateRev:
		t = CompareTargetUnion{CreateRevision: target.CreateRev}
	case FieldMod:
		t = CompareTargetUnion{ModRevision: target.ModRev}
	case FieldValue:
		t = CompareTargetUnion{Value: target.Value}
	}

	return eval(c.Operator, c.TargetField, t, c.TargetValue)
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

type CompareTargetField string

const (
	FieldValue     CompareTargetField = "VALUE"
	FieldCreateRev CompareTargetField = "CREATE"
	FieldMod       CompareTargetField = "MOD"
	FieldVersion   CompareTargetField = "VERSION"
)

type CompareTargetUnion struct {
	Value          []byte `json:"value,omitempty"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"`
}

func eval(op ComparisonOperator, field CompareTargetField, a, b CompareTargetUnion) bool {
	switch op {
	case OperatorEqual:
		return eq(field, a, b)
	case OperatorGreaterThan:
		return gt(field, a, b)
	case OperatorGreaterEqual:
		return gt(field, a, b) || eq(field, a, b)
	case OperatorLessThan:
		return lt(field, a, b)
	case OperatorLessEqual:
		return lt(field, a, b) || eq(field, a, b)
	case OperatorNotEqual:
		return !eq(field, a, b)
	default:
		// panic(unknownOperatorMsg)
		return false // default to false instead of panicking
	}
}

func eq(target CompareTargetField, a, b CompareTargetUnion) bool {
	switch target {
	case FieldValue:
		return bytes.Equal(a.Value, b.Value)
	case FieldCreateRev:
		return a.CreateRevision == b.CreateRevision
	case FieldMod:
		return a.ModRevision == b.ModRevision
	case FieldVersion:
		return a.Version == b.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}

func gt(target CompareTargetField, a, b CompareTargetUnion) bool {
	switch target {
	case FieldValue:
		return bytes.Compare(a.Value, b.Value) > 0
	case FieldCreateRev:
		return a.CreateRevision > b.CreateRevision
	case FieldMod:
		return a.ModRevision > b.ModRevision
	case FieldVersion:
		return a.Version > b.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}

func lt(target CompareTargetField, a, b CompareTargetUnion) bool {
	switch target {
	case FieldValue:
		return bytes.Compare(a.Value, b.Value) < 0
	case FieldCreateRev:
		return a.CreateRevision < b.CreateRevision
	case FieldMod:
		return a.ModRevision < b.ModRevision
	case FieldVersion:
		return a.Version < b.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}
