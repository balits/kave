package unit

import (
	"fmt"
	"testing"

	"github.com/balits/thesis/internal/store"
)

func TestInMemorySet(t *testing.T) {
	testSet(store.NewInMemoryStore(), t)
}

func TestInMemoryGetStale(t *testing.T) {
	testGetStale(store.NewInMemoryStore(), t)
}

func TestInMemoryDelete(t *testing.T) {
	testDelete(store.NewInMemoryStore(), t)
}

func errReturnedValueDidNotMatch(expected, got any) error {
	return fmt.Errorf("returned value did not match (expected %s, got %s)", expected, got)
}

func testSet(s store.Storage, t *testing.T) {
	t.Run("store.Set", func(t *testing.T) {
		key := "foo"
		value := "bar"
		_, err := s.Set(key, []byte(value))
		if err != nil {
			t.Error(err)
		}
		returned, err := s.GetStale(key)
		if err != nil {
			t.Error(err)
		}
		if string(returned) != value {
			t.Error(errReturnedValueDidNotMatch(value, returned))
		}
	})
}

func testGetStale(s store.Storage, t *testing.T) {
	t.Run("store.Set", func(t *testing.T) {
		key := "foo"
		value := "bar"
		_, err := s.Set(key, []byte(value))
		if err != nil {
			t.Error(err)
		}
		returned, err := s.GetStale(key)
		if err != nil {
			t.Error(err)
		}
		if string(returned) != value {
			t.Error(errReturnedValueDidNotMatch(value, returned))
		}
	})
}

func testDelete(s store.Storage, t *testing.T) {
	t.Run("store.Set", func(t *testing.T) {
		key := "foo"
		value := "bar"
		_, err := s.Set(key, []byte(value))
		if err != nil {
			t.Errorf("error during set: %v", err)
		}
		returned, err := s.Delete(key)
		if err != nil {
			t.Errorf("error during delete: %v", err)
		}

		if string(returned) != value {
			t.Error(errReturnedValueDidNotMatch(value, returned))
		}
	})
}
