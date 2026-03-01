package kv

import "github.com/balits/kave/internal/storage"

const (
	// TODO: Deprecate
	BucketKV       storage.Bucket = "kv"    // the default bucket for key-value pairs
	BucketLeaseWIP storage.Bucket = "lease" // the bucket for lease-related data, WIP

	// main bucket where we store revision(main, sub) -> Entry{Key,Value,CreateRev,ModRev,Version,LeaseID}
	BucketMain storage.Bucket = "main"

	// Internal bucket for storing current_revision, compacted_revision and consistent_index
	BucketMeta storage.Bucket = "_meta"
)
