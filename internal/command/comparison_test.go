package command

import (
	"testing"

	"github.com/balits/kave/internal/types"
)

func Test_ComparisonEvalEqual(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		target     types.KvEntry
		expected   bool
	}{
		{
			name: "Equal value comparison - matching values",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("expected_value")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("expected_value"),
			},
			expected: true,
		},
		{
			name: "Equal value comparison - non-matching values",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("expected_value")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("different_value"),
			},
			expected: false,
		},
		{
			name: "Equal version comparison - matching versions",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 5,
			},
			expected: true,
		},
		{
			name: "Equal version comparison - non-matching versions",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 3,
			},
			expected: false,
		},
		{
			name: "Equal create revision comparison - matching",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldCreate,
				TargetUnion: CompareTargetValue{
					CreateRevision: intPtr(100),
				},
			},
			target: types.KvEntry{
				Key:       []byte("test_key"),
				CreateRev: 100,
			},
			expected: true,
		},
		{
			name: "Equal mod revision comparison - matching",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldMod,
				TargetUnion: CompareTargetValue{
					ModRevision: intPtr(200),
				},
			},
			target: types.KvEntry{
				Key:    []byte("test_key"),
				ModRev: 200,
			},
			expected: true,
		},
		{
			name: "Equal comparison - empty values",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte(""),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.Eval(tt.target)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_ComparisonEvalNotEqual(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		target     types.KvEntry
		expected   bool
	}{
		{
			name: "Not equal value comparison - different values",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorNotEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("value1")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("value2"),
			},
			expected: true,
		},
		{
			name: "Not equal value comparison - same values",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorNotEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("value1")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("value1"),
			},
			expected: false,
		},
		{
			name: "Not equal version comparison - different versions",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorNotEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 3,
			},
			expected: true,
		},
		{
			name: "Not equal version comparison - same versions",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorNotEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 5,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.Eval(tt.target)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_ComparisonEvalGreaterThan(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		target     types.KvEntry
		expected   bool
	}{
		{
			name: "Greater than value comparison - target is greater",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorGreaterThan,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("aaa")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("zzz"),
			},
			expected: true,
		},
		{
			name: "Greater than value comparison - target is not greater",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorGreaterThan,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("zzz")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("aaa"),
			},
			expected: false,
		},
		{
			name: "Greater than value comparison - equal values",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorGreaterThan,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("equal")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("equal"),
			},
			expected: false,
		},
		{
			name: "Greater than version comparison - target version greater",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorGreaterThan,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 10,
			},
			expected: true,
		},
		{
			name: "Greater than create revision comparison",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorGreaterThan,
				Target:   FieldCreate,
				TargetUnion: CompareTargetValue{
					CreateRevision: intPtr(100),
				},
			},
			target: types.KvEntry{
				Key:       []byte("test_key"),
				CreateRev: 150,
			},
			expected: true,
		},
		{
			name: "Greater than mod revision comparison",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorGreaterThan,
				Target:   FieldMod,
				TargetUnion: CompareTargetValue{
					ModRevision: intPtr(200),
				},
			},
			target: types.KvEntry{
				Key:    []byte("test_key"),
				ModRev: 150,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.Eval(tt.target)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_ComparisonEvalGreaterOrEqual(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		target     types.KvEntry
		expected   bool
	}{
		{
			name: "Greater or equal value comparison - target greater",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorGreaterEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("aaa")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("zzz"),
			},
			expected: true,
		},
		{
			name: "Greater or equal value comparison - target equal",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorGreaterEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("equal")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("equal"),
			},
			expected: true,
		},
		{
			name: "Greater or equal value comparison - target less",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorGreaterEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("zzz")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("aaa"),
			},
			expected: false,
		},
		{
			name: "Greater or equal version comparison - target greater",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorGreaterEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 10,
			},
			expected: true,
		},
		{
			name: "Greater or equal version comparison - target equal",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorGreaterEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 5,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.Eval(tt.target)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_ComparisonEvalLessThan(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		target     types.KvEntry
		expected   bool
	}{
		{
			name: "Less than value comparison - target is less",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorLessThan,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("zzz")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("aaa"),
			},
			expected: true,
		},
		{
			name: "Less than value comparison - target is not less",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorLessThan,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("aaa")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("zzz"),
			},
			expected: false,
		},
		{
			name: "Less than version comparison - target version less",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorLessThan,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(10),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 5,
			},
			expected: true,
		},
		{
			name: "Less than create revision comparison",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorLessThan,
				Target:   FieldCreate,
				TargetUnion: CompareTargetValue{
					CreateRevision: intPtr(150),
				},
			},
			target: types.KvEntry{
				Key:       []byte("test_key"),
				CreateRev: 100,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.Eval(tt.target)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_ComparisonEvalLessOrEqual(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		target     types.KvEntry
		expected   bool
	}{
		{
			name: "Less or equal value comparison - target less",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorLessEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("zzz")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("aaa"),
			},
			expected: true,
		},
		{
			name: "Less or equal value comparison - target equal",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorLessEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("equal")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("equal"),
			},
			expected: true,
		},
		{
			name: "Less or equal value comparison - target greater",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorLessEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("aaa")},
			},
			target: types.KvEntry{
				Key:   []byte("test_key"),
				Value: []byte("zzz"),
			},
			expected: false,
		},
		{
			name: "Less or equal version comparison - target less",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorLessEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(10),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 5,
			},
			expected: true,
		},
		{
			name: "Less or equal version comparison - target equal",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorLessEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(5),
				},
			},
			target: types.KvEntry{
				Key:     []byte("test_key"),
				Version: 5,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.Eval(tt.target)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_MatchZeroValue(t *testing.T) {
	tests := []struct {
		name       string
		comparison Comparison
		expected   bool
	}{
		{
			name: "Zero value comparison - empty value field",
			comparison: Comparison{
				Key:         []byte("test_key"),
				Operator:    OperatorEqual,
				Target:      FieldValue,
				TargetUnion: CompareTargetValue{Value: []byte("")},
			},
			expected: true, // Emptytypes.Entry has empty value
		},
		{
			name: "Zero value comparison - version equals zero",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(0),
				},
			},
			expected: true, // Emptytypes.Entry has version 0
		},
		{
			name: "Zero value comparison - create revision equals zero",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldCreate,
				TargetUnion: CompareTargetValue{
					CreateRevision: intPtr(0),
				},
			},
			expected: true, // Emptytypes.Entry has CreateRev 0
		},
		{
			name: "Zero value comparison - mod revision equals zero",
			comparison: Comparison{
				Key:      []byte("test_key"),
				Operator: OperatorEqual,
				Target:   FieldMod,
				TargetUnion: CompareTargetValue{
					ModRevision: intPtr(0),
				},
			},
			expected: true, // Emptytypes.Entry has ModRev 0
		},
		{
			name: "Zero value comparison - key doesn't exist (version != 0)",
			comparison: Comparison{
				Key:      []byte("nonexistent"),
				Operator: OperatorNotEqual,
				Target:   FieldVersion,
				TargetUnion: CompareTargetValue{
					Version: intPtr(0),
				},
			},
			expected: false, // Should match zero value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.comparison.EvalEmpty()
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func Test_ComparisonOperatorConstants(t *testing.T) {
	operators := []ComparisonOperator{
		OperatorEqual,
		OperatorGreaterThan,
		OperatorGreaterEqual,
		OperatorLessThan,
		OperatorLessEqual,
		OperatorNotEqual,
	}

	expectedStrings := []string{"=", ">", ">=", "<", "<=", "!="}

	for i, op := range operators {
		if string(op) != expectedStrings[i] {
			t.Errorf("operator %d: got %q, want %q", i, op, expectedStrings[i])
		}
	}
}

func Test_CompareTargetFieldConstants(t *testing.T) {
	targets := []CompareTargetField{
		FieldValue,
		FieldCreate,
		FieldMod,
		FieldVersion,
	}

	expectedStrings := []string{"VALUE", "CREATE", "MOD", "VERSION"}

	for i, field := range targets {
		if string(field) != expectedStrings[i] {
			t.Errorf("field %d: got %q, want %q", i, field, expectedStrings[i])
		}
	}
}

func Test_InvalidOperator(t *testing.T) {
	comparison := Comparison{
		Key:         []byte("test_key"),
		Operator:    ComparisonOperator("invalid_op"),
		Target:      FieldValue,
		TargetUnion: CompareTargetValue{Value: []byte("test")},
	}

	result := comparison.Eval(types.KvEntry{
		Key:   []byte("test_key"),
		Value: []byte("test"),
	})

	if result {
		t.Errorf("expected false for invalid operator, got true")
	}
}

func Test_ComplexTransactionComparisons(t *testing.T) {
	tests := []struct {
		name        string
		comparisons []Comparison
		target      types.KvEntry
		allMatch    bool
	}{
		{
			name: "Multiple comparisons all succeed",
			comparisons: []Comparison{
				{
					Key:         []byte("key1"),
					Operator:    OperatorEqual,
					Target:      FieldValue,
					TargetUnion: CompareTargetValue{Value: []byte("value1")},
				},
				{
					Key:      []byte("key1"),
					Operator: OperatorGreaterThan,
					Target:   FieldVersion,
					TargetUnion: CompareTargetValue{
						Version: intPtr(0),
					},
				},
			},
			target: types.KvEntry{
				Key:     []byte("key1"),
				Value:   []byte("value1"),
				Version: 5,
			},
			allMatch: true,
		},
		{
			name: "Multiple comparisons - one fails",
			comparisons: []Comparison{
				{
					Key:         []byte("key1"),
					Operator:    OperatorEqual,
					Target:      FieldValue,
					TargetUnion: CompareTargetValue{Value: []byte("value1")},
				},
				{
					Key:      []byte("key1"),
					Operator: OperatorEqual,
					Target:   FieldVersion,
					TargetUnion: CompareTargetValue{
						Version: intPtr(10),
					},
				},
			},
			target: types.KvEntry{
				Key:     []byte("key1"),
				Value:   []byte("value1"),
				Version: 5,
			},
			allMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allMatch := true
			for _, cmp := range tt.comparisons {
				if !cmp.Eval(tt.target) {
					allMatch = false
					break
				}
			}

			if allMatch != tt.allMatch {
				t.Errorf("got %v, want %v", allMatch, tt.allMatch)
			}
		})
	}
}

func intPtr(v int64) *int64 {
	return &v
}
