package meta

import "github.com/balits/kave/internal/storage"

// BucketMeta is the name of the bucket where MVCC metadata is stored.
// _meta/current_revision -> backup of the current revision
// _meta/consistent_index -> mvcc revision <-> raft log index mapping
// _meta/compacted_revision -> the revision up to which compaction has been done
const BucketMeta storage.Bucket = "_meta"
