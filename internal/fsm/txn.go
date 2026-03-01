package fsm

import (
	"errors"
)

var (
	ErrTxnOpType  error = errors.New("transactions can only contain SET or DELETE operations")
	ErrTxnOpField error = errors.New("transaction operation must have a key for DELETE and both key and value for SET")
)

type Txn struct {
	Compares []Condition // list of comparisons to evaluate as the condition
	Success  []TxnOp     // list of commands to run if the condition holds, only of type SET or DELETE
	Failure  []TxnOp     // list of commands to run if the condition does not hold, only of type SET or DELETE
}

func NewTxn(compares []Condition, success []TxnOp, failure []TxnOp) *Txn {
	return &Txn{
		Compares: compares,
		Success:  success,
		Failure:  failure,
	}
}

type TxnOp struct {
	Type TxnOpType // either Set or Delete
	// Bucket storage.Bucket // bucket to operate on: Not needed, cuz transactions are only supported on the kv bucket
	Key   []byte
	Value []byte // for Set operations
}

type TxnOpType int

const (
	TxnOpGet TxnOpType = iota
	TxnOpSet
	TxnOpDelete
)

type TxnResult struct {
	Succeeded bool `json:"succeeded"`

	// A list of results corresponding to the results from applying
	// the Success block if succeeded is true or the Failure if succeeded is false.
	Results []TxnOpResult `json:"results,omitempty"`
}

// TxnOpResult is a wrapper for the result of a transaction operation,
// which can be either a Set or a Delete operation, therefore the result is either a [SetResult] or a [DeleteResult]
type TxnOpResult struct {
	GetResult    *TxnOpGetResult
	SetResult    *TxnOpSetResult
	DeleteResult *TxnOpDeleteResult
}

type TxnOpGetResult struct {
	Result GetResult
	Err    error
}

type TxnOpSetResult struct {
	Result SetResult
	Err    error
}

type TxnOpDeleteResult struct {
	Result *DeleteResult
	Err    error
}

func ValidateTxn(txn *Txn) error {
	for _, cmp := range txn.Compares {
		if cmp.Target != TargetVersion && cmp.Target != TargetCreate && cmp.Target != TargetMod && cmp.Target != TargetValue {
			return ErrInvalidCompareTarget
		}
		if cmp.Operator != CompareEqual && cmp.Operator != CompareGreater && cmp.Operator != CompareLess && cmp.Operator != CompareNotEqual {
			return ErrInvalidCompareOperator
		}

		ValidateCompare(&cmp)
	}

	for _, op := range txn.Success {
		switch op.Type {
		case TxnOpDelete:
			if op.Key == nil {
				return ErrTxnOpField
			}
		case TxnOpSet:
			if op.Key == nil || op.Value == nil {
				return ErrTxnOpField
			}
		case TxnOpGet:
			if op.Key == nil {
				return ErrTxnOpField
			}
		default:
			return ErrTxnOpType
		}

	}

	for _, op := range txn.Failure {
		switch op.Type {
		case TxnOpDelete:
			if op.Key == nil {
				return ErrTxnOpField
			}
		case TxnOpSet:
			if op.Key == nil || op.Value == nil {
				return ErrTxnOpField
			}
		default:
			return ErrTxnOpType
		}

	}

	return nil
}
