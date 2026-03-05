package durable

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"

	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	"github.com/balits/kave/internal/util"
	bolt "go.etcd.io/bbolt"
)

type boltStore struct {
	db   *bolt.DB
	path string
	// mapping from string buckets to byte buckets
	// very low overhead since there isnt a bunch of buckes anyways
	// but this way dont gotta cast (allocate) on every operation
	bucketMap      map[storage.Bucket][]byte
	storageMetrics metrics.StorageMetricsAtomic
}

func NewStore(opts storage.StorageOptions) (bytestore.ByteStore, error) {
	if opts.Kind != storage.StorageKindBoltdb {
		panic("failed to create boltdb store: option 'Kind' was not StorageKindBoltdb")
	}

	dbpath := filepath.Join(opts.Dir, "bolt.db")
	db, err := bolt.Open(dbpath, 0600, nil)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	bucketMap := make(map[storage.Bucket][]byte, len(opts.InitialBuckets))
	for _, bucket := range opts.InitialBuckets {
		tx.CreateBucket([]byte(bucket))
		bucketMap[bucket] = []byte(bucket)
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &boltStore{
		db:             db,
		path:           dbpath,
		bucketMap:      bucketMap,
		storageMetrics: metrics.StorageMetricsAtomic{},
	}, nil
}

func (s *boltStore) Close() error {
	return s.db.Close()
}

func (s *boltStore) Get(bucket storage.Bucket, key []byte) (value []byte, err error) {
	s.storageMetrics.GetCount.Add(1)
	err = s.db.View(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		value = copyBytes(bucket.Get(key))
		return nil
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}

		return
	}

	return
}

func (s *boltStore) Put(bucket storage.Bucket, key, value []byte) error {
	s.storageMetrics.SetCount.Add(1)
	if len(key) == 0 {
		return storage.ErrEmptyKey
	}

	var oldValue []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		oldValue = copyBytes(bucket.Get(key))
		return bucket.Put(key, value)
	})

	// wrap any non bucket errors into ErrInternalStorageError,
	// so that the caller can decide if it wants to retry or not based on the error type
	if err != nil && err != storage.ErrBucketNotFound {
		return fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
	} else if err != nil {
		return err
	}

	if oldValue != nil {
		oldSize := len(oldValue)
		s.storageMetrics.ByteSize.Add(uint64(len(value) - oldSize))
	} else {
		s.storageMetrics.ByteSize.Add(uint64(len(value) + len(key)))
		s.storageMetrics.KeyCount.Add(1)
	}

	return nil
}

func (s *boltStore) Delete(bucket storage.Bucket, key []byte) (value []byte, err error) {
	s.storageMetrics.DeleteCount.Add(1)
	err = s.db.Update(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		value = copyBytes(bucket.Get(key))
		return bucket.Delete(key)
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}

		return
	}

	if value == nil {
		return
	}

	// only subtract if there was an actual value in the db previously
	util.AtomicSubNoUnderflow(&s.storageMetrics.KeyCount, 1)
	util.AtomicSubNoUnderflow(&s.storageMetrics.ByteSize, uint64(len(key)+len(value)))
	return
}

func (s *boltStore) PrefixScan(bucket storage.Bucket, prefix []byte, f func(key, value []byte) bool) error {
	g := func(key, value []byte) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}

		if !f(key, value) {
			return false
		}
		return true
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if !g(copyBytes(k), copyBytes(v)) {
				continue
			}
		}

		return nil
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}
		return err
	}

	return nil
}

func (s *boltStore) Scan(bucket storage.Bucket, f func(key, value []byte) bool) error {
	err := s.db.View(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if !f(copyBytes(k), copyBytes(v)) {
				break
			}
		}

		return nil
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}
		return err
	}

	return nil
}

func (s *boltStore) NewBatch() (bytestore.Batch, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
	}
	return newBatch(tx, s.bucketMap), nil
}

func (s *boltStore) WriteTo(w io.Writer) (int64, error) {
	return 0, encode(w, s) // we dont really know how many bytes were written, and it doesnt matter for us, so we just return 0
}

func (s *boltStore) ReadFrom(r io.Reader) (int64, error) {
	db, err := decode(r, s.path)
	if err != nil {
		return 0, err // we dont really know how many bytes were read, and it doesnt matter for us, so we just return 0
	}

	s.db = db
	return 0, nil // we dont really know how many bytes were read, and it doesnt matter for us, so we just return 0
}

func (s *boltStore) Defragment() error {
	newPath := s.path + ".defrag"

	// use BoltDB built-in copy to compact
	db, err := bolt.Open(s.path, 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(newPath, 0600)
	})
}

func (s *boltStore) Compact(bucket storage.Bucket, shouldDelete func([]byte) bool) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		c := bucket.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if shouldDelete(k) {
				// err shouldnt happen since we dont have nested buckets
				// and its a writable tx
				if err := c.Delete(); err != nil {
					return fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
				}
			}
		}

		return nil
	})
}

// ========= metrics.StorageMetricsProvider impl =========
func (s *boltStore) StorageMetrics() *metrics.StorageMetrics {
	return s.storageMetrics.StorageMetrics()
}

func copyBytes(in []byte) (out []byte) {
	if in == nil {
		return nil
	}
	return append(out, in...)
}
