package schema

import "github.com/balits/kave/internal/storage"

const (
	// Append only history we store revision(main, sub) -> Entry{Key,Value,CreateRev,ModRev,Version,LeaseID}
	BucketKV storage.Bucket = "kv"

	// Internal bucket for storing current revision and compacted revision.
	BucketMeta storage.Bucket = "_meta"

	// The bucket for lease-related data
	BucketLease storage.Bucket = "lease"

	// The bucket for secret management using Oblivious Transfer
	BucketOT storage.Bucket = "ot"
)

var AllBuckets = []storage.Bucket{BucketKV, BucketMeta, BucketLease, BucketOT}
