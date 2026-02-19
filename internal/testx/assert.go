package testx

import (
	"bytes"
	"testing"
)

func AssertEq[T comparable](tb testing.TB, a, b T) {
	if a != b {
		tb.Fatalf("assertion failed: expected %v got %v", a, b)
	}
}

func AssertEqBytes(tb testing.TB, a, b []byte) {
	if !bytes.Equal(a, b) {
		tb.Fatalf("assertion failed: expected %v got %v", string(a), string(b))
	}
}
