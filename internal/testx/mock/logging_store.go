package mock

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/metrics"
	"github.com/balits/thesis/internal/store"
	"github.com/hashicorp/raft"
)

func NewLoggingStore(storage store.Storage) store.Storage {
	return &LoggingStore{
		Inner: storage,
		Logs:  make([]command.Command, 0),
	}
}

// MockFSMStore is an implementation of the FSM interface, and just stores
// the logs sequentially.
// It also implements store.KVStore so its compatible with raftnode.Node
type LoggingStore struct {
	Inner store.Storage
	Logs  []command.Command
	sync.Mutex
}

type LoggerFsmSnapshot struct {
	snapshot raft.FSMSnapshot
	logs     []command.Command
	maxIndex int
}

// Snapshot implements raft.FSM
func (l *LoggingStore) Snapshot() (raft.FSMSnapshot, error) {
	l.Lock()
	logs := append([]command.Command{}, l.Logs...)
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
		Logs []command.Command
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

func (l *LoggingStore) Set(key []byte, value []byte) error {
	l.Logs = append(l.Logs, command.Command{
		Type:  command.CommandTypeSet,
		Key:   string(key),
		Value: value,
	})
	return l.Inner.Set(key, value)
}

func (l *LoggingStore) Delete(key []byte) (value []byte, err error) {
	panic("store.Storage.Delete not implemented")
}

func (l *LoggingStore) Get(key []byte) (value []byte, err error) {
	return l.Inner.Get(key)
}

func (l *LoggingStore) StorageMetrics() *metrics.StorageMetrics {
	panic("store.(StorageMetricsProvider).StorageMetrics() not implemented")
}

func (l *LoggingStore) PrefixScan(prefix []byte) ([]store.KVItem, error) {
	panic("store.PrefixScan() not implemented")
}

func (l *LoggingStore) NewBatch() (store.Batch, error) {
	return l.Inner.NewBatch()
}

func (l *LoggingStore) Close() error {
	return l.Inner.Close()
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
		Logs []command.Command
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
