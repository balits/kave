package durable

import (
	"bytes"
	"io"

	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/store"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

type Store struct {
	db             *bolt.DB
	path           string
	storageMetrics metrics.StorageMetricsAtomic
}

func NewStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	tx.CreateBucket([]byte(store.BucketKV))
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &Store{
		db:             db,
		path:           path,
		storageMetrics: metrics.StorageMetricsAtomic{},
	}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Get(bucket store.Bucket, key []byte) (value []byte, err error) {
	s.storageMetrics.GetCount.Add(1)

	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return store.ErrBucketNotFound
		}

		value = copyBytes(bucket.Get(key))
		return nil
	})

	if err != nil {
		return
	}

	if value == nil {
		err = store.ErrKeyNotFound
	}

	return
}

func (s *Store) Set(bucket store.Bucket, key, value []byte) error {
	s.storageMetrics.SetCount.Add(1)
	var oldValue []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return store.ErrBucketNotFound
		}

		oldValue = copyBytes(bucket.Get(key))
		return bucket.Put(key, value)
	})

	if err != nil {
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

func (s *Store) Delete(bucket store.Bucket, key []byte) (value []byte, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return store.ErrBucketNotFound
		}

		value = copyBytes(bucket.Get(key))
		return bucket.Delete(key)
	})

	if err != nil {
		return
	}

	if value == nil {
		return
	}

	// only subtract if there was a non nil value in the db previously
	util.AtomicSubNoUnderflow(&s.storageMetrics.KeyCount, 1)
	util.AtomicSubNoUnderflow(&s.storageMetrics.ByteSize, uint64(len(key)+len(value)))
	return
}

func (s *Store) PrefixScan(prefix []byte) ([]store.KV, error) {
	result := make([]store.KV, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(store.BucketKV))
		if bucket == nil {
			return store.ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		k, v := cursor.Seek(prefix)
		for {
			if k == nil || !bytes.HasPrefix(k, prefix) {
				break
			}

			result = append(result, store.KV{
				Key:   copyBytes(k),
				Value: copyBytes(v),
			})

			k, v = cursor.Next()
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Store) NewBatch() (store.Batch, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return newBatch(tx), nil
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	return Snapshot{s}, nil
}

func (s *Store) Restore(sink io.ReadCloser) error {
	defer sink.Close()

	db, err := Decode(sink, s.path)
	if err != nil {
		return err
	}

	s.db = db
	return nil
}

// ========= metrics.StorageMetricsProvider impl =========
func (s *Store) StorageMetrics() *metrics.StorageMetrics {
	return s.storageMetrics.StorageMetrics()
}

func copyBytes(in []byte) (out []byte) {
	return append(out, in...)
}
