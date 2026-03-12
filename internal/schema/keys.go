package schema

// MetaKey constants for the _meta bucket
type MetaKey []byte

var (
	// Set to the index last applied log entry
	MetaKeyRaftApplyIndex MetaKey = []byte("consistent_index")

	// Set to the term last applied log entry
	MetaKeyRaftTerm MetaKey = []byte("term")

	MetaKeyCurrentRevision MetaKey = []byte("current_revision")

	// Stores the last requested compaction revision.
	// Stored at request, before compaction is actually run
	MetaKeyScheduledCompactionRev MetaKey = []byte("scheduled_compaction_rev")

	// Stores the last finished compaction revision.
	// Stored after compaction is finished
	MetaKeyFinishedCompactionRev MetaKey = []byte("finished_compaction_rev")
)
