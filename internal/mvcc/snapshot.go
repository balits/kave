package mvcc

import "github.com/hashicorp/raft"

type Snapshot struct {
	store *KVStore
}

func (s Snapshot) Persist(sink raft.SnapshotSink) error {
	s.store.rwlock.RLock()
	defer s.store.rwlock.RUnlock()
	if err := s.store.backend.Snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (s Snapshot) Release() {}
