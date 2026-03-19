package mvcc

import (
	"bytes"
	"log/slog"
	"os"
	"testing"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
)

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

// newTestEngineWithAttacher creates an Engine wired with a fakeAttacher.
func newTestEngineWithAttacher() (*Engine, *KVStore, *fakeAttacher) {
	e, s := newTestEngine()
	fa := &fakeAttacher{}
	e.attacher = fa
	return e, s, fa
}

func newTestEngine() (*Engine, *KVStore) {
	reg := metrics.InitTestPrometheus()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	s := NewKVStore(reg, logger, b)
	return &Engine{store: s}, s
}

// ==================== Engine.Apply PUT ====================

func Test_EngineApplyPut(t *testing.T) {
	e, _ := newTestEngine()
	defer e.store.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.PutCmd{Key: []byte("foo"), Value: []byte("bar")},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Header.Revision != 1 {
		t.Errorf("rev = %d, want 1", result.Header.Revision)
	}
	if result.Put == nil {
		t.Fatal("Put result is nil")
	}
	if result.Put.PrevEntry != nil {
		t.Error("PrevEntry should be nil when not requested")
	}
}

func Test_EngineApplyPutMultiple(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("c"), Value: []byte("3")}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Header.Revision != 3 {
		t.Errorf("rev = %d, want 3", result.Header.Revision)
	}
	currRev, _ := s.Revisions()
	if currRev.Main != 3 {
		t.Errorf("store rev = %d, want 3", currRev.Main)
	}
}

func Test_EngineApplyPutOverwrite(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := s.NewReader()
	entries, _, _, _ := r.Range([]byte("k"), nil, 0, 0)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("v2")) {
		t.Errorf("value = %q, want %q", entries[0].Value, "v2")
	}
}

func Test_EngineApplyPutWithPrevEntry(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.PutCmd{Key: []byte("k"), Value: []byte("v2"), PrevEntry: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Put == nil {
		t.Fatal("Put result is nil")
	}
	if result.Put.PrevEntry == nil {
		t.Fatal("PrevEntry should not be nil when requested")
	}
	if !bytes.Equal(result.Put.PrevEntry.Value, []byte("v1")) {
		t.Errorf("PrevEntry.Value = %q, want %q", result.Put.PrevEntry.Value, "v1")
	}
}

func Test_EngineApplyPutWithPrevEntryNonExistent(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindPut,
		Put:  &command.PutCmd{Key: []byte("new"), Value: []byte("val"), PrevEntry: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Put.PrevEntry != nil {
		t.Errorf("PrevEntry should be nil for new key, got %+v", result.Put.PrevEntry)
	}
}

// ==================== Engine.Apply DELETE ====================

func Test_EngineApplyDeleteSingleKey(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("foo"), Value: []byte("bar")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.DeleteCmd{Key: []byte("foo")},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Delete == nil {
		t.Fatal("Delete result is nil")
	}
	if result.Delete.NumDeleted != 1 {
		t.Errorf("deleted = %d, want 1", result.Delete.NumDeleted)
	}
	if result.Header.Revision != 2 {
		t.Errorf("rev = %d, want 2", result.Header.Revision)
	}
	entry := e.store.NewReader().Get([]byte("foo"), 0)
	if entry != nil {
		t.Error("expected key to be deleted, but it still exists")
	}
}

func Test_EngineApplyDeleteNonExistent(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.DeleteCmd{Key: []byte("nope")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Delete.NumDeleted != 0 {
		t.Errorf("deleted = %d, want 0", result.Delete.NumDeleted)
	}
}

func Test_EngineApplyDeleteRange(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("c"), Value: []byte("3")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("d"), Value: []byte("4")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.DeleteCmd{Key: []byte("b"), End: []byte("d")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Delete.NumDeleted != 2 {
		t.Errorf("deleted = %d, want 2 (b and c)", result.Delete.NumDeleted)
	}
}

func Test_EngineApplyDeleteWithPrevEntries(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("x"), Value: []byte("xv")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("y"), Value: []byte("yv")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.DeleteCmd{Key: []byte("x"), End: []byte("z"), PrevEntries: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Delete.NumDeleted != 2 {
		t.Errorf("deleted = %d, want 2", result.Delete.NumDeleted)
	}
	if len(result.Delete.PrevEntries) != 2 {
		t.Fatalf("PrevEntries = %d, want 2", len(result.Delete.PrevEntries))
	}
	if !bytes.Equal(result.Delete.PrevEntries[0].Value, []byte("xv")) {
		t.Errorf("PrevEntries[0].Value = %q, want %q", result.Delete.PrevEntries[0].Value, "xv")
	}
}

func Test_EngineApplyDeleteWithPrevEntriesNonExistent(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind:   command.KindDelete,
		Delete: &command.DeleteCmd{Key: []byte("nope"), PrevEntries: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Delete.NumDeleted != 0 {
		t.Errorf("deleted = %d, want 0", result.Delete.NumDeleted)
	}
	if len(result.Delete.PrevEntries) != 0 {
		t.Errorf("PrevEntries should be empty, got %d", len(result.Delete.PrevEntries))
	}
}

// ==================== Engine.Apply TXN — success branch ====================

func Test_EngineApplyTxn_SuccessBranch(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("counter"), Value: []byte("hello")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("counter"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldVersion,
					TargetUnion: command.CompareTargetValue{Version: intPtr(1)},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("counter"), Value: []byte("updated")}},
			},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("counter"), Value: []byte("failed")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Txn == nil {
		t.Fatal("TxnResult is nil")
	}
	if !result.Txn.Success {
		t.Error("expected success branch")
	}

	r := s.NewReader()
	entries, _, _, _ := r.Range([]byte("counter"), nil, 0, 0)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("updated")) {
		t.Errorf("value = %q, want %q", entries[0].Value, "updated")
	}
}

// ==================== Engine.Apply TXN — failure branch ====================

func Test_EngineApplyTxn_FailureBranch(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("counter"), Value: []byte("hello")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("counter"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldVersion,
					TargetUnion: command.CompareTargetValue{Version: intPtr(99)},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("counter"), Value: []byte("should_not")}},
			},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("counter"), Value: []byte("failed_path")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Txn == nil {
		t.Fatal("TxnResult is nil")
	}
	if result.Txn.Success {
		t.Error("expected failure branch")
	}

	r := s.NewReader()
	entries, _, _, _ := r.Range([]byte("counter"), nil, 0, 0)
	if !bytes.Equal(entries[0].Value, []byte("failed_path")) {
		t.Errorf("value = %q, want %q", entries[0].Value, "failed_path")
	}
}

func Test_EngineApplyTxn_NoComparisonsAlwaysSuccess(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: nil,
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("no comparisons should always succeed")
	}

	r := s.NewReader()
	entries, _, _, _ := r.Range([]byte("k"), nil, 0, 0)
	if len(entries) != 1 {
		t.Errorf("entries = %d, want 1", len(entries))
	}
}

func Test_EngineApplyTxn_EmptyOps(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: nil,
			Success:     nil,
			Failure:     nil,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("expected success with empty comparisons")
	}
	if len(result.Txn.Results) != 0 {
		t.Errorf("results = %d, want 0", len(result.Txn.Results))
	}
	_ = s
}

func Test_EngineApplyTxn_WithDeleteOp(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k1"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k2"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Success: []command.TxnOp{
				{Type: command.TxnOpDelete, Delete: &command.DeleteCmd{Key: []byte("k1")}},
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("k3"), Value: []byte("v3")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("expected success")
	}

	r := s.NewReader()
	e1, _, _, _ := r.Range([]byte("k1"), nil, 0, 0)
	e3, _, _, _ := r.Range([]byte("k3"), nil, 0, 0)
	if len(e1) != 0 {
		t.Error("k1 should be deleted")
	}
	if len(e3) != 1 {
		t.Error("k3 should exist")
	}
}

func Test_EngineApplyTxn_CompareNonExistentKey(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("missing"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldVersion,
					TargetUnion: command.CompareTargetValue{Version: intPtr(0)},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("missing"), Value: []byte("created")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("comparing version==0 for non-existent key should succeed")
	}

	r := s.NewReader()
	entries, _, _, _ := r.Range([]byte("missing"), nil, 0, 0)
	if len(entries) != 1 || !bytes.Equal(entries[0].Value, []byte("created")) {
		t.Error("key should have been created by success branch")
	}
}

// ==================== Engine.Apply TXN — multiple comparisons ====================

func Test_EngineApplyTxn_MultipleComparisonsAllPass(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{Key: []byte("a"), Operator: command.OperatorEqual, Target: command.FieldVersion, TargetUnion: command.CompareTargetValue{Version: intPtr(1)}},
				{Key: []byte("b"), Operator: command.OperatorEqual, Target: command.FieldVersion, TargetUnion: command.CompareTargetValue{Version: intPtr(1)}},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("result"), Value: []byte("both_matched")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("both comparisons should pass")
	}
	_ = s
}

func Test_EngineApplyTxn_OneComparisonFails(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{Key: []byte("a"), Operator: command.OperatorEqual, Target: command.FieldVersion, TargetUnion: command.CompareTargetValue{Version: intPtr(1)}},
				{Key: []byte("b"), Operator: command.OperatorEqual, Target: command.FieldVersion, TargetUnion: command.CompareTargetValue{Version: intPtr(99)}},
			},
			Success: []command.TxnOp{},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("result"), Value: []byte("one_failed")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Txn.Success {
		t.Error("one comparison failed, should take failure branch")
	}
	_ = s
}

// ==================== Engine.Apply TXN — PrevEntry inside txn ops ====================

func Test_EngineApplyTxn_PutWithPrevEntry(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("old")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("new"), PrevEntry: true}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Fatal("expected success")
	}
	if len(result.Txn.Results) < 1 {
		t.Fatal("expected at least 1 result")
	}

	putRes := result.Txn.Results[0].Put
	if putRes == nil {
		t.Fatal("Put result is nil")
	}
	if putRes.PrevEntry == nil {
		t.Fatal("PrevEntry should not be nil when requested inside txn")
	}
	if !bytes.Equal(putRes.PrevEntry.Value, []byte("old")) {
		t.Errorf("PrevEntry.Value = %q, want %q", putRes.PrevEntry.Value, "old")
	}
	_ = s
}

func Test_EngineApplyTxn_DeleteWithPrevEntries(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("x"), Value: []byte("xv")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Success: []command.TxnOp{
				{Type: command.TxnOpDelete, Delete: &command.DeleteCmd{Key: []byte("x"), PrevEntries: true}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Txn.Results) < 1 {
		t.Fatal("expected at least 1 result")
	}
	delRes := result.Txn.Results[0].Delete
	if delRes == nil {
		t.Fatal("Delete result is nil")
	}
	if delRes.NumDeleted != 1 {
		t.Errorf("deleted = %d, want 1", delRes.NumDeleted)
	}
	if len(delRes.PrevEntries) != 1 {
		t.Fatalf("PrevEntries = %d, want 1", len(delRes.PrevEntries))
	}
	if !bytes.Equal(delRes.PrevEntries[0].Value, []byte("xv")) {
		t.Errorf("PrevEntries[0].Value = %q, want %q", delRes.PrevEntries[0].Value, "xv")
	}
	_ = s
}

func Test_EngineApplyTxn_MixedOps(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("c"), Value: []byte("3")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Success: []command.TxnOp{
				{Type: command.TxnOpDelete, Delete: &command.DeleteCmd{Key: []byte("a")}},
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("updated")}},
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("d"), Value: []byte("new")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("expected success")
	}

	r := s.NewReader()
	a, _, _, _ := r.Range([]byte("a"), nil, 0, 0)
	b, _, _, _ := r.Range([]byte("b"), nil, 0, 0)
	c, _, _, _ := r.Range([]byte("c"), nil, 0, 0)
	d, _, _, _ := r.Range([]byte("d"), nil, 0, 0)

	if len(a) != 0 {
		t.Error("a should be deleted")
	}
	if len(b) != 1 || !bytes.Equal(b[0].Value, []byte("updated")) {
		t.Errorf("b = %v, want updated", b)
	}
	if len(c) != 1 || !bytes.Equal(c[0].Value, []byte("3")) {
		t.Error("c should be untouched")
	}
	if len(d) != 1 || !bytes.Equal(d[0].Value, []byte("new")) {
		t.Error("d should be created")
	}
}

func Test_EngineApplyTxn_CompareValue(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("expected")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldValue,
					TargetUnion: command.CompareTargetValue{Value: []byte("expected")},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("matched")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("value comparison should succeed")
	}
	_ = s
}

func Test_EngineApplyTxn_CompareValueMismatch(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("actual")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldValue,
					TargetUnion: command.CompareTargetValue{Value: []byte("wrong")},
				},
			},
			Success: []command.TxnOp{},
			Failure: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("result"), Value: []byte("mismatch")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Txn.Success {
		t.Error("value comparison should fail")
	}
	_ = s
}

func Test_EngineApplyTxn_CompareCreateRev(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldCreate,
					TargetUnion: command.CompareTargetValue{CreateRevision: intPtr(1)},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("check"), Value: []byte("createRev_ok")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("createRev should be 1 despite update at rev 2")
	}
	_ = s
}

func Test_EngineApplyTxn_CompareModRev(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(command.Command{Kind: command.KindPut, Put: &command.PutCmd{Key: []byte("k"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Comparisons: []command.Comparison{
				{
					Key:         []byte("k"),
					Operator:    command.OperatorEqual,
					Target:      command.FieldMod,
					TargetUnion: command.CompareTargetValue{ModRevision: intPtr(2)},
				},
			},
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("check"), Value: []byte("modRev_ok")}},
			},
			Failure: []command.TxnOp{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Txn.Success {
		t.Error("modRev should be 2 after second put")
	}
	_ = s
}

func Test_EngineApplyUnknownCommandError(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()
	_, err := e.ApplyWrite(command.Command{Kind: "UNKNOWN"})
	if err == nil {
		t.Error("expected error for unknown command type")
	}
}

func Test_EngineApplyTxn_ResultCount(t *testing.T) {
	e, s := newTestEngine()
	defer s.Close()

	result, err := e.ApplyWrite(command.Command{
		Kind: command.KindTxn,
		Txn: &command.TxnCmd{
			Success: []command.TxnOp{
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}},
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}},
				{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("c"), Value: []byte("3")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Txn.Results) != 3 {
		t.Errorf("results = %d, want 3 (one per op)", len(result.Txn.Results))
	}
	for i, r := range result.Txn.Results {
		if r.Put == nil {
			t.Errorf("results[%d].Put is nil", i)
		}
	}
	_ = s
}

func intPtr(v int64) *int64 { return &v }
