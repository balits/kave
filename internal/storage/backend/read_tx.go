package backend

import (
	"bytes"
	"fmt"

	"github.com/balits/kave/internal/storage"
)

// TODO: read error prefix: ??
// ReadTx is a read only transaction over the backend. Its RLock() and RUnlock() use the backends rwlock,
// so multiple reads can happen at a given time.
type ReadTx interface {
	RLock()
	RUnlock()

	// UnsafeGet returns the value for the given key in the given bucket, or an error if the key doesn't exist.
	UnsafeGet(bucket storage.Bucket, key []byte) ([]byte, error)

	// UnsafeRange iterates over all key-value pairs in the [start, end) range and applies the given function to them,
	// returning each value in a slice. The function can return an error to stop the iteration and return it.
	UnsafeRange(bucket storage.Bucket, start, end []byte, f func(k, v []byte) error) (res [][]byte, err error)

	// UnsafeScan iterates over all key-value pairs in the [start, end) range and applies the given function to them.
	// The function can return an error to stop the iteration and return it.
	UnsafeScan(bucket storage.Bucket, start, end []byte, f func(k, v []byte) error) error
}

type readtx struct {
	b *backend
}

func (r *readtx) RLock()   { r.b.rwlock.RLock() }
func (r *readtx) RUnlock() { r.b.rwlock.RUnlock() }

func (r *readtx) UnsafeGet(bucket storage.Bucket, key []byte) ([]byte, error) {
	return r.b.store.Get(bucket, key)
}

func (r *readtx) UnsafeRange(bucket storage.Bucket, start, end []byte, f func(k, v []byte) error) (res [][]byte, err error) {
	var fErr error
	err = r.b.store.Scan(bucket, func(k, v []byte) bool {
		if (start == nil || bytes.Compare(k, start) >= 0) &&
			(end == nil || bytes.Compare(k, end) < 0) {
			if err := f(k, v); err != nil {
				fErr = err
				return false
			}
			res = append(res, v)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if fErr != nil {
		return nil, fErr
	}

	return
}

func (r *readtx) UnsafeScan(bucket storage.Bucket, start, end []byte, f func(k, v []byte) error) error {
	var err error
	scanErr := r.b.store.Scan(bucket, func(k, v []byte) bool {
		if (start == nil || bytes.Compare(k, start) >= 0) &&
			(end == nil || bytes.Compare(k, end) < 0) {
			if err = f(k, v); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil || scanErr != nil {
		return fmt.Errorf("UnsafeScan failed: '%v' (inner error: '%v')", scanErr, err)
	}
	return nil
}
