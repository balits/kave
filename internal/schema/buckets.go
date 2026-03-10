package schema

import "github.com/balits/kave/internal/storage"

const (
	// Append only history we store revision(main, sub) -> Entry{Key,Value,CreateRev,ModRev,Version,LeaseID}
	BucketKV storage.Bucket = "kv"

	// Internal bucket for storing current_revision, compacted_revision and consistent_index
	BucketMeta storage.Bucket = "_meta"

	// The bucket for lease-related data, WIP
	BucketLeaseWIP storage.Bucket = "lease"
)

var AllBuckets = []storage.Bucket{BucketKV, BucketMeta, BucketLeaseWIP}
