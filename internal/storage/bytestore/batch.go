package bytestore

import "github.com/balits/kave/internal/storage"

type Batch interface {
	// Put and Delete record the mutations in the batch, but do not apply them to the underlying store until Commit is called
	Put(bucket storage.Bucket, key, value []byte) error
	// Put and Delete record the mutations in the batch, but do not apply them to the underlying store until Commit is called
	Delete(bucket storage.Bucket, key []byte) error
	// Commmit applies all recorded mutations to the underlying store atomically. After Commit is called, the batch is closed and cannot be used anymore.
	Commit() (storage.CommitInfo, error)
	// Abort discards all recorded mutations and closes the batch. After Abort is called, the batch cannot be used anymore.
	Abort()
}
