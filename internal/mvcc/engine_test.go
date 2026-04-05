package mvcc

import (
	"log/slog"
	"os"
	"testing"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types/api"
	"github.com/stretchr/testify/require"
)

// fakeAttacher is kept only for tests that specifically need to assert
// whether AttachKey / DetachKey were called. For everything else use
// the real LeaseManager via newTestEngineWithLeases.
type fakeAttacher struct {
	attached []attachCall
	detached []detachCall
}

type attachCall struct {
	leaseID int64
	key     []byte
}

type detachCall struct {
	leaseID int64
	key     []byte
}

func (f *fakeAttacher) AttachKey(leaseID int64, key []byte) {
	f.attached = append(f.attached, attachCall{leaseID: leaseID, key: key})
}

func (f *fakeAttacher) DetachKey(leaseID int64, key []byte) {
	f.detached = append(f.detached, detachCall{leaseID: leaseID, key: key})
}

func (f *fakeAttacher) attachCount() int { return len(f.attached) }
func (f *fakeAttacher) detachCount() int { return len(f.detached) }

func (f *fakeAttacher) wasAttached(leaseID int64, key []byte) bool {
	for _, c := range f.attached {
		if c.leaseID == leaseID && string(c.key) == string(key) {
			return true
		}
	}
	return false
}

func (f *fakeAttacher) wasDetached(leaseID int64, key []byte) bool {
	for _, c := range f.detached {
		if c.leaseID == leaseID && string(c.key) == string(key) {
			return true
		}
	}
	return false
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	reg := metrics.InitTestPrometheus()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	s := NewKvStore(reg, logger, b)
	t.Cleanup(func() { b.Close() })
	return &Engine{store: s}
}

func newTestEngineWithAttacher(t *testing.T) (*Engine, *fakeAttacher) {
	t.Helper()
	e := newTestEngine(t)
	fa := &fakeAttacher{}
	e.attacher = fa
	return e, fa
}

func Test_EngineApplyPut(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("foo"), Value: []byte("bar")},
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), result.Header.Revision)
	require.NotNil(t, result.Put)
	require.Nil(t, result.Put.PrevEntry)
}

func Test_EngineApplyPutMultiple(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("a"), Value: []byte("1")}})
	require.NoError(t, err)

	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("b"), Value: []byte("2")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("c"), Value: []byte("3")}})
	require.NoError(t, err)

	require.Equal(t, int64(3), result.Header.Revision)
	currRev, _ := e.store.Revisions()
	require.Equal(t, int64(3), currRev.Main)
}

func Test_EngineApplyPutOverwrite(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v1")}})
	require.NoError(t, err)

	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v2")}})
	require.NoError(t, err)

	r := e.store.NewReader()
	entries, _, _, _ := r.Range([]byte("k"), nil, 0, 0)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("v2"), entries[0].Value)
}

func Test_EngineApplyPutWithPrevEntry(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v1")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("v2"), PrevEntry: true},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Put)
	require.NotNil(t, result.Put.PrevEntry)
	require.Equal(t, []byte("v1"), result.Put.PrevEntry.Value)
}

func Test_EngineApplyPutWithPrevEntryNonExistent(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("new"), Value: []byte("val"), PrevEntry: true},
	})
	require.NoError(t, err)
	require.Nil(t, result.Put.PrevEntry)
}

func Test_EngineApplyDeleteSingleKey(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("foo"), Value: []byte("bar")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.CmdDelete{Key: []byte("foo")},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Delete)
	require.Equal(t, int64(1), result.Delete.NumDeleted)
	require.Equal(t, int64(2), result.Header.Revision)

	entry := e.store.NewReader().Get([]byte("foo"), 0)
	require.Nil(t, entry)
}

func Test_EngineApplyDeleteNonExistent(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.CmdDelete{Key: []byte("nope")},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Delete)
	require.Equal(t, int64(0), result.Delete.NumDeleted)
}

func Test_EngineApplyDeleteRange(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	for _, kv := range [][]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}} {
		_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte(kv[0]), Value: []byte(kv[1])}})
		require.NoError(t, err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.CmdDelete{Key: []byte("b"), End: []byte("d")},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Delete)
	require.Equal(t, int64(2), result.Delete.NumDeleted)
}

func Test_EngineApplyDeleteWithPrevEntries(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("x"), Value: []byte("xv")}})
	require.NoError(t, err)
	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("y"), Value: []byte("yv")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.CmdDelete{Key: []byte("x"), End: []byte("z"), PrevEntries: true},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Delete.NumDeleted)
	require.Len(t, result.Delete.PrevEntries, 2)
	require.Equal(t, []byte("xv"), result.Delete.PrevEntries[0].Value)
}

func Test_EngineApplyDeleteWithPrevEntriesNonExistent(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.CmdDelete{Key: []byte("nope"), PrevEntries: true},
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Delete.NumDeleted)
	require.Empty(t, result.Delete.PrevEntries)
}

func Test_EngineApplyTxn_SuccessBranch(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("counter"), Value: []byte("hello")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("counter"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldVersion,
					TargetValue: api.CompareTargetUnion{Version: 1},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("counter"), Value: []byte("updated")}},
			},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("counter"), Value: []byte("failed")}},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Txn)
	require.True(t, result.Txn.Success)

	entries, _, _, _ := e.store.NewReader().Range([]byte("counter"), nil, 0, 0)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("updated"), entries[0].Value)
}

func Test_EngineApplyTxn_FailureBranch(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("counter"), Value: []byte("hello")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("counter"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldVersion,
					TargetValue: api.CompareTargetUnion{Version: 99},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("counter"), Value: []byte("should_not")}},
			},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("counter"), Value: []byte("failed_path")}},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Txn)
	require.False(t, result.Txn.Success)

	entries, _, _, _ := e.store.NewReader().Range([]byte("counter"), nil, 0, 0)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("failed_path"), entries[0].Value)
}

func Test_EngineApplyTxn_NoComparisonsAlwaysSuccess(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: nil,
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success)

	entries, _, _, _ := e.store.NewReader().Range([]byte("k"), nil, 0, 0)
	require.Len(t, entries, 1)
}

func Test_EngineApplyTxn_EmptyOps(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: nil,
			Success:     nil,
			Failure:     nil,
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success)
	require.Empty(t, result.Txn.Results)
	require.Zero(t, result.Header.Revision)
}

func Test_EngineApplyTxn_WithDeleteOp(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k1"), Value: []byte("v1")}})
	require.NoError(t, err)
	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k2"), Value: []byte("v2")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Success: []command.TxnOp{
				{Type: command.TxnOpDelete, Delete: &command.CmdDelete{Key: []byte("k1")}},
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("k3"), Value: []byte("v3")}},
			},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success)

	r := e.store.NewReader()
	e1, _, _, _ := r.Range([]byte("k1"), nil, 0, 0)
	e3, _, _, _ := r.Range([]byte("k3"), nil, 0, 0)
	require.Empty(t, e1, "k1 should be deleted")
	require.Len(t, e3, 1, "k3 should exist")
}

func Test_EngineApplyTxn_CompareNonExistentKey(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("missing"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldVersion,
					TargetValue: api.CompareTargetUnion{Version: 0},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("missing"), Value: []byte("created")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success, "comparing version==0 for non-existent key should succeed")

	entries, _, _, _ := e.store.NewReader().Range([]byte("missing"), nil, 0, 0)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("created"), entries[0].Value)
}

func Test_EngineApplyTxn_MultipleComparisonsAllPass(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("a"), Value: []byte("1")}})
	require.NoError(t, err)
	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("b"), Value: []byte("2")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{Key: []byte("a"), Operator: api.OperatorEqual, TargetField: api.FieldVersion, TargetValue: api.CompareTargetUnion{Version: 1}},
				{Key: []byte("b"), Operator: api.OperatorEqual, TargetField: api.FieldVersion, TargetValue: api.CompareTargetUnion{Version: 1}},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("result"), Value: []byte("both_matched")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success, "both comparisons should pass")
}

func Test_EngineApplyTxn_OneComparisonFails(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("a"), Value: []byte("1")}})
	require.NoError(t, err)
	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("b"), Value: []byte("2")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{Key: []byte("a"), Operator: api.OperatorEqual, TargetField: api.FieldVersion, TargetValue: api.CompareTargetUnion{Version: 1}},
				{Key: []byte("b"), Operator: api.OperatorEqual, TargetField: api.FieldVersion, TargetValue: api.CompareTargetUnion{Version: 99}},
			},
			Success: []command.TxnOp{},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("result"), Value: []byte("one_failed")}},
			},
		},
	})
	require.NoError(t, err)
	require.False(t, result.Txn.Success, "one comparison failed, should take failure branch")
}

func Test_EngineApplyTxn_PutWithPrevEntry(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("old")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("new"), PrevEntry: true}},
			},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success)
	require.Len(t, result.Txn.Results, 1)

	putRes := result.Txn.Results[0].Put
	require.NotNil(t, putRes)
	require.NotNil(t, putRes.PrevEntry, "PrevEntry should not be nil when requested inside txn")
	require.Equal(t, []byte("old"), putRes.PrevEntry.Value)
}

func Test_EngineApplyTxn_DeleteWithPrevEntries(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("x"), Value: []byte("xv")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Success: []command.TxnOp{
				{Type: command.TxnOpDelete, Delete: &command.CmdDelete{Key: []byte("x"), PrevEntries: true}},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Txn.Results, 1)

	delRes := result.Txn.Results[0].Delete
	require.NotNil(t, delRes)
	require.Equal(t, int64(1), delRes.NumDeleted)
	require.Len(t, delRes.PrevEntries, 1)
	require.Equal(t, []byte("xv"), delRes.PrevEntries[0].Value)
}

func Test_EngineApplyTxn_MixedOps(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	for _, kv := range [][]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte(kv[0]), Value: []byte(kv[1])}})
		require.NoError(t, err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Success: []command.TxnOp{
				{Type: command.TxnOpDelete, Delete: &command.CmdDelete{Key: []byte("a")}},
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("b"), Value: []byte("updated")}},
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("d"), Value: []byte("new")}},
			},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success)

	r := e.store.NewReader()
	a, _, _, _ := r.Range([]byte("a"), nil, 0, 0)
	b, _, _, _ := r.Range([]byte("b"), nil, 0, 0)
	c, _, _, _ := r.Range([]byte("c"), nil, 0, 0)
	d, _, _, _ := r.Range([]byte("d"), nil, 0, 0)

	require.Empty(t, a, "a should be deleted")
	require.Len(t, b, 1)
	require.Equal(t, []byte("updated"), b[0].Value)
	require.Len(t, c, 1)
	require.Equal(t, []byte("3"), c[0].Value, "c should be untouched")
	require.Len(t, d, 1)
	require.Equal(t, []byte("new"), d[0].Value)
}

func Test_EngineApplyTxn_CompareValue(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("expected")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldValue,
					TargetValue: api.CompareTargetUnion{Value: []byte("expected")},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("matched")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success, "value comparison should succeed")
}

func Test_EngineApplyTxn_CompareValueMismatch(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("actual")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldValue,
					TargetValue: api.CompareTargetUnion{Value: []byte("wrong")},
				},
			},
			Success: []command.TxnOp{},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("result"), Value: []byte("mismatch")}},
			},
		},
	})
	require.NoError(t, err)
	require.False(t, result.Txn.Success, "value comparison should fail")
}

func Test_EngineApplyTxn_CompareCreateRev(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v1")}})
	require.NoError(t, err)
	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v2")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldCreate,
					TargetValue: api.CompareTargetUnion{CreateRevision: 1},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("check"), Value: []byte("createRev_ok")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success, "createRev should be 1 despite update at rev 2")
}

func Test_EngineApplyTxn_CompareModRev(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v1")}})
	require.NoError(t, err)
	_, err = e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v2")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    api.OperatorEqual,
					TargetField: api.FieldMod,
					TargetValue: api.CompareTargetUnion{ModRevision: 2},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("check"), Value: []byte("modRev_ok")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	require.NoError(t, err)
	require.True(t, result.Txn.Success, "modRev should be 2 after second put")
}

func Test_EngineApplyUnknownCommandError(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)
	require.Panics(t, func() {
		e.ApplyWrite(command.Command{Kind: "UNKNOWN"})
	})
}

func Test_EngineApplyTxn_ResultCount(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.CmdTxn{
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("a"), Value: []byte("1")}},
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("b"), Value: []byte("2")}},
				{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("c"), Value: []byte("3")}},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Txn.Results, 3, "one result per op")
	for i, r := range result.Txn.Results {
		require.NotNil(t, r.Put, "results[%d].Put is nil", i)
	}
}
func Test_EngineApplyPut_IgnoreValue_PreservesValue(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("original")},
	})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), IgnoreValue: true},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Put)

	entry := e.store.NewReader().Get([]byte("k"), 0)
	require.NotNil(t, entry)
	require.Equal(t, "original", string(entry.Value))
}

func Test_EngineApplyPut_IgnoreValue_NonExistent_ReturnsError(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("ghost"), IgnoreValue: true},
	})

	require.Error(t, err)
}

func Test_EngineApplyPut_IgnoreValue_BumpsRevisionAndVersion(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.CmdPut{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), IgnoreValue: true},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Header.Revision)

	entry := e.store.NewReader().Get([]byte("k"), 0)
	require.Equal(t, int64(2), entry.Version)
	require.Equal(t, int64(1), entry.CreateRev, "createRev must not change")
	require.Equal(t, int64(2), entry.ModRev)
}

<<<<<<< HEAD
=======

>>>>>>> 8081303 (add(testing): parallelize testing to speed up CI)
func Test_EngineApplyPut_IgnoreLease_NonExistent_ReturnsError(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("ghost"), Value: []byte("v"), IgnoreLease: true},
	})

	require.Error(t, err)
}

func Test_EngineApplyPut_IgnoreLease_DoesNotDetachWhenLeaseUnchanged(t *testing.T) {
	t.Parallel()
	e, fa := newTestEngineWithAttacher(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("v1"), LeaseID: 99},
	})
	require.NoError(t, err)

	fa.attached = nil
	fa.detached = nil

	_, err = e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("v2"), IgnoreLease: true},
	})
	require.NoError(t, err)

	require.False(t, fa.wasDetached(99, []byte("k")),
		"lease 99 must not be detached — value changed but lease is preserved via IgnoreLease")
}

func Test_EngineApplyPut_BothIgnore_TouchDoesNotDetach(t *testing.T) {
	t.Parallel()
	e, fa := newTestEngineWithAttacher(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("v"), LeaseID: 7},
	})
	require.NoError(t, err)

	fa.attached = nil
	fa.detached = nil

	_, err = e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), IgnoreValue: true, IgnoreLease: true},
	})
	require.NoError(t, err)

	require.False(t, fa.wasDetached(7, []byte("k")), "lease must not be detached on touch")
}

func Test_EngineApplyPut_BothIgnore_NonExistent_ReturnsError(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("ghost"), IgnoreValue: true, IgnoreLease: true},
	})

	require.Error(t, err)
}

func Test_EngineApplyPut_IgnoreLease_PreservesLease(t *testing.T) {
	t.Parallel()
	e, fa := newTestEngineWithAttacher(t)

	_, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("v1"), LeaseID: 42},
	})
	require.NoError(t, err)

	_, err = e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.CmdPut{Key: []byte("k"), Value: []byte("v2"), IgnoreLease: true},
	})
	require.NoError(t, err)

	// verify through the store — no lease manager needed
	entry := e.store.NewReader().Get([]byte("k"), 0)
	require.NotNil(t, entry)
	require.Equal(t, "v2", string(entry.Value))
	require.Equal(t, int64(42), entry.LeaseID, "lease should be preserved")

	// also verify the attacher behavior
	require.False(t, fa.wasDetached(42, []byte("k")))
	require.True(t, fa.wasAttached(42, []byte("k")))
}
