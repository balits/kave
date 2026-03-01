package kv

import "github.com/balits/kave/internal/storage"

const (
	// TODO: Deprecate
	BucketKV       storage.Bucket = "kv"    // the default bucket for key-value pairs
	BucketLeaseWIP storage.Bucket = "lease" // the bucket for lease-related data, WIP

	// Up to date metadata about each key.
	// stores Key -> EntryMeta{
	// 		createRevision uint64,
	// 		modRevision uint64,
	// 		version uint64,
	// 		tombstone bool
	// }
	BucketKeyMeta storage.Bucket = "key_meta"

	// Append only historical log of all version of a key.
	// stores CompositeKey{key, revision} -> value []byte
	BucketKeyHistory storage.Bucket = "key_history"

	// Internal bucket for storing current_revision, compacted_revision and consistent_index
	BucketMeta storage.Bucket = "_meta"
)
