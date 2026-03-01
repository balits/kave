package schema

import "github.com/balits/kave/internal/storage"

const (
	// Append only log of state transitions (== revisions == writes) and their result
	// (mainRev, subRev) -> Entry{
	// 		key []byte,
	// 		value []byte,
	// 		createRev uint64,
	// 		modRev uint64,
	// 		version uint64,
	// 		leaseID uint64,
	// 		tombstone bool
	// }
	BucketKV storage.Bucket = "kv"

	// bucket for store level metadata like current_revision, compacted_revision and consistent_index
	BucketMeta storage.Bucket = "_meta"

	// bucket for lease-related data, WIP
	BucketLeaseWIP storage.Bucket = "lease"
)

var AllBuckets = []storage.Bucket{BucketKV, BucketMeta, BucketLeaseWIP}

var (
	KeyCurrentRevision   = []byte("current_revision")   // latest revision number, incremented on every write
	KeyCompactedRevision = []byte("compacted_revision") // latest compacted revision number, updated on compaction
	KeyConsistentIndex   = []byte("consistent_index")   // mapping between revision and Raft log index, updated on every write
)
