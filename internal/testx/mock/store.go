package mock

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/balits/thesis/internal/metrics"
	"github.com/balits/thesis/internal/store"
	"github.com/hashicorp/raft"
)

func NewLoggingStore(storage store.Storage) *LoggingStore {
	return &LoggingStore{
		Inner: storage,
		Logs:  make([]store.Cmd, 0),
	}
}

// MockFSMStore is an implementation of the FSM interface, and just stores
// the logs sequentially.
// It also implements store.KVStore so its compatible with raftnode.Node
type LoggingStore struct {
	Inner store.Storage
	Logs  []store.Cmd
	sync.Mutex
}

type LoggerFsmSnapshot struct {
	snapshot raft.FSMSnapshot
	logs     []store.Cmd
	maxIndex int
}

// Snapshot implements raft.FSM
func (l *LoggingStore) Snapshot() (raft.FSMSnapshot, error) {
	l.Lock()
	logs := append([]store.Cmd{}, l.Logs...)
	l.Unlock()

	snap, err := l.Inner.Snapshot()
	if err != nil {
		return nil, err
	}

	return &LoggerFsmSnapshot{
		snapshot: snap,
		logs:     logs,
		maxIndex: len(logs),
	}, nil
}

// Restore implements raft.FSM
func (l *LoggingStore) Restore(inp io.ReadCloser) error {
	var snap struct {
		Logs []store.Cmd
		Data []byte
	}

	if err := gob.NewDecoder(inp).Decode(&snap); err != nil {
		return err
	}
	l.Lock()
	l.Logs = snap.Logs
	l.Unlock()
	r := bytes.NewReader(snap.Data)
	return l.Inner.Restore(io.NopCloser(r))
}

func (l *LoggingStore) Set(key string, value []byte) (int, error) {
	l.Logs = append(l.Logs, store.Cmd{
		Kind:  store.CmdKindSet,
		Key:   key,
		Value: value,
	})
	return l.Inner.Set(key, value)
}

// Mutation -> through raft
func (l *LoggingStore) Delete(key string) (value []byte, err error) {
	panic("store.Storage.Delete not implemented")
}

// Query -> local read, may be stale (since it doesnt go through raft)
func (l *LoggingStore) GetStale(key string) (value []byte, err error) {
	panic("store.Storage.GetStale not implemented")
}

func (l *LoggingStore) StorageMetrics() *metrics.StorageMetrics {
	panic("store.(StorageMetricsProvider).StorageMetrics() not implemented")
}

func (s *LoggerFsmSnapshot) Persist(sink raft.SnapshotSink) error {
	var buf bytes.Buffer
	tmpSink := &mockSnapshotSink{Writer: &buf}

	if err := s.snapshot.Persist(tmpSink); err != nil {
		sink.Cancel()
		return err
	}

	// Encode both logs and underlying snapshot data
	if err := gob.NewEncoder(sink).Encode(struct {
		Logs []store.Cmd
		Data []byte
	}{
		Logs: s.logs,
		Data: buf.Bytes(),
	}); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *LoggerFsmSnapshot) Release() {
	s.snapshot.Release()
}

type mockSnapshotSink struct {
	io.Writer
}

func (m *mockSnapshotSink) Close() error  { return nil }
func (m *mockSnapshotSink) Cancel() error { return nil }
func (m *mockSnapshotSink) ID() string    { return "mock" }
