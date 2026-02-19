package unit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"testing"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/store/durable"
	"github.com/balits/thesis/internal/store/inmem"
	"github.com/balits/thesis/internal/testx"
	"github.com/balits/thesis/internal/util"
)

func Test_InMemoryStorage(t *testing.T) {
	NewStorageTester(inmem.NewStore(), "InMemoryStorage", nil).Run(t)
}

func Test_InDurableStorage(t *testing.T) {
	path := t.TempDir() + "/bolt.db"
	store, err := durable.NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	NewStorageTester(store, "DurableStorage", &path).Run(t)
}

var DEFAULT_VALUE = b("default_value")

type StorageTester struct {
	store      store.Storage
	id         string
	dummyState map[string][]byte
	path       string
	t          *testing.T
}

func NewStorageTester(store store.Storage, id string, path *string) *StorageTester {
	n := 100
	m := make(map[string][]byte, n)
	for i := range n {
		key := fmt.Sprintf("key%d", i)
		m[key] = DEFAULT_VALUE
	}

	var p string
	if path != nil {
		p = *path
	}

	return &StorageTester{
		store:      store,
		id:         id,
		dummyState: m,
		path:       p,
	}
}

func (s *StorageTester) Run(t *testing.T) {
	f := func(str string) string {
		return fmt.Sprintf("%s_%s", s.id, str)
	}
	t.Run(f("SET"), s.testSet)
	t.Run(f("DELETE"), s.testDelete)
	t.Run(f("PREFIX_SCAN"), s.testPrefixScan)
	t.Run(f("BATCH"), s.testBatch)
	t.Run(f("CODEC"), s.testCodec)
}

// we use store.GetStale in other testcases already, no need to test it on its own
func (st *StorageTester) testGetStale(_ *testing.T) { util.Todo("test GET_STALE") }

func (st *StorageTester) testSet(t *testing.T) {
	for key, value := range st.dummyState {
		err := st.store.Set(b(key), value)
		if err != nil {
			t.Fatalf("SET failed: %v", err)
		}
	}

	for key, expected := range st.dummyState {
		got, err := st.store.Get(b(key))
		if err != nil {
			t.Fatalf("GET_STALE failed: %v", err)
		}

		testx.AssertEqBytes(t, expected, got)
	}
}

func (st *StorageTester) testDelete(t *testing.T) {
	n := 10
	for i := range n {
		key := b(fmt.Sprintf("delete%d", i))
		err := st.store.Set(key, DEFAULT_VALUE)
		if err != nil {
			t.Fatalf("SET failed: %v", err)
		}
	}

	for i := range n {
		key := b(fmt.Sprintf("delete%d", i))
		got, err := st.store.Delete(key)
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		testx.AssertEqBytes(t, DEFAULT_VALUE, got)
	}
}

func (st *StorageTester) testBatch(t *testing.T) {
	prefix := b("batch_")
	val := slices.Concat(prefix, DEFAULT_VALUE)
	commands := []command.Command{
		{Type: command.CommandTypeSet, Key: []byte("batch_set1"), Value: val, BatchOps: nil, ExpectedRevision: nil},
		{Type: command.CommandTypeSet, Key: []byte("batch_set2"), Value: val, BatchOps: nil, ExpectedRevision: nil},
		{Type: command.CommandTypeSet, Key: []byte("batch_set3"), Value: val, BatchOps: nil, ExpectedRevision: nil},
		{Type: command.CommandTypeSet, Key: []byte("batch_set4"), Value: val, BatchOps: nil, ExpectedRevision: nil}, // only this will survice after the batch
		{Type: command.CommandTypeDelete, Key: []byte("batch_set1"), Value: val, BatchOps: nil, ExpectedRevision: nil},
		{Type: command.CommandTypeDelete, Key: []byte("batch_set2"), Value: val, BatchOps: nil, ExpectedRevision: nil},
		{Type: command.CommandTypeDelete, Key: []byte("batch_set3"), Value: val, BatchOps: nil, ExpectedRevision: nil},
	}

	expectedState := map[string][]byte{
		"batch_set4": val,
	}

	// clean up prev matching keys
	for _, cmd := range commands {
		if cmd.Type != command.CommandTypeSet {
			continue
		}
		_, err := st.store.Delete([]byte(cmd.Key))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
	}

	if err := applyBatch(st.store, commands); err != nil {
		t.Fatalf("BATCH failed: %v", err)
	}

	result, err := st.store.PrefixScan(prefix)
	if err != nil {
		t.Fatalf("PREFIX_SCAN failed: %v", err)
	}

	for _, kv := range result {
		v, ok := expectedState[string(kv.Key)]
		if !ok {
			t.Fatalf("expected key %s to be in post-batch state", kv.Key)
		} else {
			testx.AssertEqBytes(t, v, kv.Value)
		}
	}

}

func (st *StorageTester) testPrefixScan(t *testing.T) {
	salt := rand.Intn(3)
	n := 10
	for i := range n {
		key := b(fmt.Sprintf("%dprefix%d", salt, i))
		err := st.store.Set(key, DEFAULT_VALUE)
		if err != nil {
			t.Fatalf("SET failed: %v", err)
		}
	}

	test := func(prefix []byte, expectedSize int) {
		result, err := st.store.PrefixScan(prefix)
		if err != nil {
			t.Fatalf("PREFIX_SCAN failed: %v", err)
		}

		for _, item := range result {
			t.Logf("%s - %s", item.Key, item.Value)
		}

		testx.AssertEq(t, expectedSize, len(result))
		for _, item := range result {
			if !bytes.HasPrefix(item.Key, prefix) {
				t.Fatalf("SET failed: %s does not have the prefix %s", item.Key, prefix)
			}
		}
	}

	var (
		prefix       []byte
		expectedSize int
	)

	prefix = b(fmt.Sprintf("%dp", salt))
	expectedSize = n
	t.Logf("prefix %s, expecting %d number of result", prefix, expectedSize)
	test(prefix, expectedSize)

	prefix = b(fmt.Sprintf("%dpre", salt))
	expectedSize = n
	t.Logf("prefix %s, expecting %d number of result", prefix, expectedSize)
	test(prefix, expectedSize)

	prefix = b(fmt.Sprintf("%dprefix", salt))
	expectedSize = n
	t.Logf("prefix %s, expecting %d number of result", prefix, expectedSize)
	test(prefix, expectedSize)

	prefix = b(fmt.Sprintf("%d_prefix", salt))
	expectedSize = 0
	t.Logf("prefix %s, expecting %d number of result", prefix, expectedSize)
	test(prefix, expectedSize)

	prefix = b(fmt.Sprintf("%dprefix0", salt))
	expectedSize = 1
	t.Logf("prefix %s, expecting %d number of result", prefix, expectedSize)
	test(prefix, expectedSize)

	// cleanup
	for i := range n {
		key := b(fmt.Sprintf("%dprefix%d", salt, i))
		_, err := st.store.Delete(key)
		if err != nil {
			t.Fatalf("CLEANUP DELETE failed: %v", err)
		}
	}
}

func (st *StorageTester) testCodec(t *testing.T) {
	items1 := make([]string, 0)
	items2 := make([]string, 0)

	kvitems1, err := st.store.PrefixScan([]byte(""))
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvitems1 {
		items1 = append(items1, string(slices.Concat(kv.Key, kv.Value)))
	}

	sink := newSink("test_sink")
	snap, err := st.store.Snapshot()
	if err != nil {
		t.Fatalf("store.Snapshot(): %v", err)
	}

	err = snap.Persist(sink)
	if err != nil {
		t.Fatalf("snapshot.Persist(): %v", err)
	}
	fmt.Println(len(items1), len(sink.Bytes()), sink.Bytes())
	err = st.store.Restore(io.NopCloser(bytes.NewBuffer(sink.Bytes())))

	if err != nil {
		t.Fatalf("store.Restore(): %v", err)
	}

	kvitems2, err := st.store.PrefixScan([]byte(""))
	if err != nil {
		t.Fatal(err)
	}

	for _, kv := range kvitems2 {
		items2 = append(items2, string(slices.Concat(kv.Key, kv.Value)))
	}

	if !slices.Equal(items1, items2) {
		t.Fatalf("items not equal\n\nitems1:\n%+v\n\nitems2:\n%+v\n", items1, items2)
	}

}

func b(s string) []byte {
	return []byte(s)
}

func s(b []byte) string {
	return string(b)
}

type sink struct {
	id        string
	buf       bytes.Buffer
	cancelled bool
	closed    bool
}

func newSink(id string) *sink {
	return &sink{id: id}
}

func (s *sink) Write(p []byte) (int, error) {
	if s.cancelled {
		return 0, errors.New("snapshot cancelled")
	}
	if s.closed {
		return 0, errors.New("snapshot already closed")
	}
	return s.buf.Write(p)
}

func (s *sink) Close() error {
	if s.cancelled {
		return errors.New("cannot close cancelled snapshot")
	}
	s.closed = true
	return nil
}

func (s *sink) Cancel() error {
	s.cancelled = true
	s.buf.Reset()
	return nil
}

func (s *sink) Bytes() []byte {
	return s.buf.Bytes()
}

func (s *sink) ID() string {
	return s.id
}

func applyBatch(store store.Storage, commands []command.Command) (err error) {
	batch, err := store.NewBatch()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, batch.Abort())
		}
	}()

	for _, cmd := range commands {
		switch cmd.Type {
		case command.CommandTypeSet:
			err = batch.Set([]byte(cmd.Key), cmd.Value)
			if err != nil {
				return
			}
		case command.CommandTypeDelete:
			err = batch.Delete([]byte(cmd.Key))
			if err != nil {
				return
			}
		}
	}

	err = batch.Commit()
	return
}
