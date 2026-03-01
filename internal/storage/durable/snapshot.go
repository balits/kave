package durable

// import (
// 	"errors"

// 	"github.com/hashicorp/raft"
// )

// // Snapshot is a snapshot of the data at a given time, used to support log compaction and restore the FSM to a desired state.
// // It is returned FSM.Snapshot() which itself shouldn't do heavy IO work.
// // No mutex needed since snapshots are immutable
// //
// // since [durableStore] uses boltdb, which in turn uses MVCC,
// // any read only view of the DB is gonna be consistent
// type Snapshot struct {
// 	store *boltStore
// }

// // Persist should dump all necessary state to the WriteCloser 'sink',
// // and call sink.Close() when finished or call sink.Cancel() on error.
// func (s Snapshot) Persist(sink raft.SnapshotSink) error {
// 	if err := encode(sink, s); err != nil {
// 		err2 := sink.Cancel()
// 		if err2 != nil {
// 			return errors.Join(err, err2)
// 		}
// 		return err
// 	}

// 	return sink.Close()
// }

// // Release is invoked when we are finished with the snapshot.
// func (s Snapshot) Release() {}
