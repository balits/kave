package durable

import (
	"io"
	"os"

	bolt "go.etcd.io/bbolt"
)

// Encode is responsible for writing the snapshot
// to the given writer, preferably a file.
// It streams memory pages from file (bolt.db) to file (writer),
// making it very memory efficient
func Encode(w io.Writer, snapshot Snapshot) error {
	return snapshot.store.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

// Decode is responsible for building up the bolt db from
// the given reader, preferably a file,
func Decode(r io.Reader, oldPath string) (*bolt.DB, error) {
	tmpfile := oldPath + ".restore"
	f, err := os.Create(tmpfile)
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return nil, err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}

	if err := f.Close(); err != nil {
		return nil, err
	}

	// atomic on same fs (on unix)
	if err := os.Rename(tmpfile, oldPath); err != nil {
		return nil, err
	}

	return bolt.Open(oldPath, 0600, nil)
}
