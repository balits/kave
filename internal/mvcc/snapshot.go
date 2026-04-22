package mvcc

import "github.com/hashicorp/raft"

type Snapshot struct {
	store *KvStore
}

func (s Snapshot) Persist(sink raft.SnapshotSink) error {
	s.store.storeMu.RLock()
	defer s.store.storeMu.RUnlock()
	if err := s.store.backend.Snapshot(sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}

func (s Snapshot) Release() {}
