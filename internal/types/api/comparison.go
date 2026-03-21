package api

import (
	"bytes"
	"fmt"

	"github.com/balits/kave/internal/types"
)

// TODO: no checks for union field
type Comparison struct {
	Key         []byte             `json:"key"`          // key of the value ot compare
	Operator    ComparisonOperator `json:"operator"`     // logical operator to apply
	Target      CompareTargetField `json:"target_field"` // which field to compare against
	TargetUnion CompareTargetUnion `json:"target_value"` // actual value of Target
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
	switch c.Target {
	case FieldValue, FieldCreate, FieldMod, FieldVersion:
		// ok
	default:
		return fmt.Errorf("invalid comparison target field: %s", c.Target)
	}
	return nil
}

func (c *Comparison) Eval(target *types.KvEntry) (result bool) {
	if target == nil {
		target = &types.KvEntry{}
	}

	var t CompareTargetUnion

	switch c.Target {
	case FieldVersion:
		t = CompareTargetUnion{Version: &target.Version}
	case FieldCreate:
		t = CompareTargetUnion{CreateRevision: &target.CreateRev}
	case FieldMod:
		t = CompareTargetUnion{ModRevision: &target.ModRev}
	case FieldValue:
		t = CompareTargetUnion{Value: target.Value}
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

type CompareTargetField string

const (
	FieldValue   CompareTargetField = "VALUE"
	FieldCreate  CompareTargetField = "CREATE"
	FieldMod     CompareTargetField = "MOD"
	FieldVersion CompareTargetField = "VERSION"
)

type CompareTargetUnion struct {
	Value          []byte `json:"value,omitempty"`
	CreateRevision *int64 `json:"create_revision,omitempty"`
	ModRevision    *int64 `json:"mod_revision,omitempty"`
	Version        *int64 `json:"version,omitempty"`
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

func eq(target CompareTargetField, op1, op2 CompareTargetUnion) bool {
	switch target {
	case FieldValue:
		return bytes.Equal(op1.Value, op2.Value)
	case FieldCreate:
		return *op1.CreateRevision == *op2.CreateRevision
	case FieldMod:
		return *op1.ModRevision == *op2.ModRevision
	case FieldVersion:
		return *op1.Version == *op2.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}

func gt(target CompareTargetField, op1, op2 CompareTargetUnion) bool {
	switch target {
	case FieldValue:
		return bytes.Compare(op1.Value, op2.Value) > 0
	case FieldCreate:
		return *op1.CreateRevision > *op2.CreateRevision
	case FieldMod:
		return *op1.ModRevision > *op2.ModRevision
	case FieldVersion:
		return *op1.Version > *op2.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}

func lt(target CompareTargetField, op1, op2 CompareTargetUnion) bool {
	switch target {
	case FieldValue:
		return bytes.Compare(op1.Value, op2.Value) < 0
	case FieldCreate:
		return *op1.CreateRevision < *op2.CreateRevision
	case FieldMod:
		return *op1.ModRevision < *op2.ModRevision
	case FieldVersion:
		return *op1.Version < *op2.Version
	default:
		// panic(unknownTargetMsg)
		return false // default to false instead of panicking
	}
}
