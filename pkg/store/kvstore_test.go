package store

import (
	"fmt"
	"testing"
)

func TestInMemorySet(t *testing.T) {
	testSet(NewInMemoryStore(), t)
}

func TestInMemoryGetStale(t *testing.T) {
	testGetStale(NewInMemoryStore(), t)
}

func TestInMemoryDelete(t *testing.T) {
	testDelete(NewInMemoryStore(), t)
}

func errReturnedValueDidNotMatch(expected, got any) error {
	return fmt.Errorf("returned value did not match (expected %s, got %s)", expected, got)
}

func testSet(s KVStore, t *testing.T) {
	t.Run("store.Set", func(t *testing.T) {
		key := "foo"
		value := "bar"
		err := s.Set(key, []byte(value))
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

func testGetStale(s KVStore, t *testing.T) {
	t.Run("store.Set", func(t *testing.T) {
		key := "foo"
		value := "bar"
		err := s.Set(key, []byte(value))
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

func testDelete(s KVStore, t *testing.T) {
	t.Run("store.Set", func(t *testing.T) {
		key := "foo"
		value := "bar"
		err := s.Set(key, []byte(value))
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
