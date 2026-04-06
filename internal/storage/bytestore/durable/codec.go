package durable

import (
	"fmt"
	"io"
	"os"

	bolt "go.etcd.io/bbolt"
)

// encode is responsible for writing the snapshot
// to the given writer, preferably a file.
// It streams memory pages from file (bolt.db) to file (writer),
// making it very memory efficient
func encode(w io.Writer, store *boltStore) error {
	return store.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

// decode is responsible for building up the bolt db from
// the given reader, preferably a file,
func decode(r io.Reader, oldPath string) (*bolt.DB, error) {
	tmpfile := oldPath + ".restore"
	f, err := os.Create(tmpfile)
	if err != nil {
		return nil, fmt.Errorf("failed to create tmpfile: %v", err)
	}

	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to copy files: %v", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to sync file: %v", err)
	}

	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("failed to close file: %v", err)
	}

	if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove file: %v", err)
	}

	// atomic on same fs (on unix)
	if err := os.Rename(tmpfile, oldPath); err != nil {
		return nil, fmt.Errorf("failed to rename file: %v", err)
	}

	db, err := bolt.Open(oldPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create boltdb: %v", err)
	}

	return db, nil
}
