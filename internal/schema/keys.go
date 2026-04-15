package schema

// NamedKey is a key used to access
// frequently used values in the store
type NamedKey []byte

// keys in BucketMeta
var (
	// Set to the index last applied log entry
	KeyRaftApplyIndex NamedKey = []byte("consistent_index")

	// Set to the term last applied log entry
	KeyRaftTerm NamedKey = []byte("term")

	KeyCurrentRevision NamedKey = []byte("current_revision")

	// Stores the last requested compaction revision.
	// Stored at request time, before compaction is actually run
	KeyScheduledCompactedRev NamedKey = []byte("scheduled_compacted_rev")

	// Stores the last finished compaction revision.
	// Stored rhgt after compaction is finished successfully
	KeyFinishedCompactedRev NamedKey = []byte("finished_compacted_rev")
)

// keys in BucketOT
var (
	// Stores all the slots inserted to the OT bucket as single opaque byte blob.
	KeyOTBlob NamedKey = []byte("blob")

	// Stores the cluster wide secret key used for OT
	KeyOTClusterKey NamedKey = []byte("cluster_key")
)
