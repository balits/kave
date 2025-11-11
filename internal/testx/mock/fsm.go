package mock

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/balits/thesis/internal/store"
	"github.com/hashicorp/raft"
)

func NewLoggingFSM(fsm store.FSMStore) *LoggingFsm {
	return &LoggingFsm{
		Fsm:  fsm,
		Logs: make([]store.Cmd, 0),
	}
}

// MockFSMStore is an implementation of the FSM interface, and just stores
// the logs sequentially.
// It also implements store.KVStore so its compatible with raftnode.Node
type LoggingFsm struct {
	Fsm  store.FSMStore
	Logs []store.Cmd
	sync.Mutex
}

type LoggerFsmSnapshot struct {
	snapshot raft.FSMSnapshot
	logs     []store.Cmd
	maxIndex int
}

// Apply implements raft.FSM
func (l *LoggingFsm) Apply(log *raft.Log) interface{} {
	var cmd store.Cmd
	if err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&cmd); err != nil {
		return store.NewApplyResponse(cmd, err)
	}
	l.Lock()
	l.Logs = append(l.Logs, cmd)
	l.Unlock()
	return l.Fsm.Apply(log)
}

// Snapshot implements raft.FSM
func (l *LoggingFsm) Snapshot() (raft.FSMSnapshot, error) {
	l.Lock()
	logs := append([]store.Cmd{}, l.Logs...)
	l.Unlock()

	snap, err := l.Fsm.Snapshot()
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
func (l *LoggingFsm) Restore(inp io.ReadCloser) error {
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
	return l.Fsm.Restore(io.NopCloser(r))
}

func (l *LoggingFsm) Set(key string, value []byte) error { return l.Fsm.Set(key, value) }

// Mutation -> through raft
func (l *LoggingFsm) Delete(key string) (value []byte, err error) { return l.Fsm.Delete(key) }

// Query -> local read, may be stale (since it doesnt go through raft)
func (l *LoggingFsm) GetStale(key string) (value []byte, err error) { return l.Fsm.GetStale(key) }

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
