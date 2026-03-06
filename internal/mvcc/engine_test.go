package mvcc

import (
	"bytes"
	"testing"

	"github.com/balits/kave/internal/kv"
)

func newTestEngine() (*Engine, *KVStore) {
	s := newTestKVStore()
	return &Engine{store: s}, s
}

// ==================== Engine.Apply PUT ====================

func Test_EngineApplyPut(t *testing.T) {
	e, _ := newTestEngine()
	defer e.store.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdPut,
		Put:  &kv.PutCmd{Key: []byte("foo"), Value: []byte("bar")},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("c"), Value: []byte("3")}})
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v2")}}); err != nil {
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdPut,
		Put:  &kv.PutCmd{Key: []byte("k"), Value: []byte("v2"), PrevEntry: true},
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
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdPut,
		Put:  &kv.PutCmd{Key: []byte("new"), Value: []byte("val"), PrevEntry: true},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("foo"), Value: []byte("bar")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := e.ApplyWrite(kv.Command{
		Type:   kv.CmdDelete,
		Delete: &kv.DeleteCmd{Key: []byte("foo")},
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
}

func Test_EngineApplyDeleteNonExistent(t *testing.T) {
	e, s := newTestEngine()
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type:   kv.CmdDelete,
		Delete: &kv.DeleteCmd{Key: []byte("nope")},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("c"), Value: []byte("3")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("d"), Value: []byte("4")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type:   kv.CmdDelete,
		Delete: &kv.DeleteCmd{Key: []byte("b"), End: []byte("d")},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("x"), Value: []byte("xv")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("y"), Value: []byte("yv")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type:   kv.CmdDelete,
		Delete: &kv.DeleteCmd{Key: []byte("x"), End: []byte("z"), PrevEntries: true},
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
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type:   kv.CmdDelete,
		Delete: &kv.DeleteCmd{Key: []byte("nope"), PrevEntries: true},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("counter"), Value: []byte("hello")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("counter"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldVersion,
					TargetUnion: kv.CompareTargetValue{Version: intPtr(1)},
				},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("counter"), Value: []byte("updated")}},
			},
			Failure: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("counter"), Value: []byte("failed")}},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("counter"), Value: []byte("hello")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("counter"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldVersion,
					TargetUnion: kv.CompareTargetValue{Version: intPtr(99)},
				},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("counter"), Value: []byte("should_not")}},
			},
			Failure: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("counter"), Value: []byte("failed_path")}},
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
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: nil,
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v")}},
			},
			Failure: []kv.TxnOp{},
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
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k1"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k2"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Success: []kv.TxnOp{
				{Type: kv.TxnOpDelete, Delete: &kv.DeleteCmd{Key: []byte("k1")}},
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("k3"), Value: []byte("v3")}},
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
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("missing"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldVersion,
					TargetUnion: kv.CompareTargetValue{Version: intPtr(0)},
				},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("missing"), Value: []byte("created")}},
			},
			Failure: []kv.TxnOp{},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{Key: []byte("a"), Operator: kv.OperatorEqual, Target: kv.FieldVersion, TargetUnion: kv.CompareTargetValue{Version: intPtr(1)}},
				{Key: []byte("b"), Operator: kv.OperatorEqual, Target: kv.FieldVersion, TargetUnion: kv.CompareTargetValue{Version: intPtr(1)}},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("result"), Value: []byte("both_matched")}},
			},
			Failure: []kv.TxnOp{},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{Key: []byte("a"), Operator: kv.OperatorEqual, Target: kv.FieldVersion, TargetUnion: kv.CompareTargetValue{Version: intPtr(1)}},
				{Key: []byte("b"), Operator: kv.OperatorEqual, Target: kv.FieldVersion, TargetUnion: kv.CompareTargetValue{Version: intPtr(99)}},
			},
			Success: []kv.TxnOp{},
			Failure: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("result"), Value: []byte("one_failed")}},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("old")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("new"), PrevEntry: true}},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("x"), Value: []byte("xv")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Success: []kv.TxnOp{
				{Type: kv.TxnOpDelete, Delete: &kv.DeleteCmd{Key: []byte("x"), PrevEntries: true}},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("a"), Value: []byte("1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("c"), Value: []byte("3")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Success: []kv.TxnOp{
				{Type: kv.TxnOpDelete, Delete: &kv.DeleteCmd{Key: []byte("a")}},
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("updated")}},
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("d"), Value: []byte("new")}},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("expected")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("k"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldValue,
					TargetUnion: kv.CompareTargetValue{Value: []byte("expected")},
				},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("matched")}},
			},
			Failure: []kv.TxnOp{},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("actual")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("k"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldValue,
					TargetUnion: kv.CompareTargetValue{Value: []byte("wrong")},
				},
			},
			Success: []kv.TxnOp{},
			Failure: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("result"), Value: []byte("mismatch")}},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("k"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldCreate,
					TargetUnion: kv.CompareTargetValue{CreateRevision: intPtr(1)},
				},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("check"), Value: []byte("createRev_ok")}},
			},
			Failure: []kv.TxnOp{},
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
	defer s.backend.Close()

	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := e.ApplyWrite(kv.Command{Type: kv.CmdPut, Put: &kv.PutCmd{Key: []byte("k"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Comparisons: []kv.Comparison{
				{
					Key:         []byte("k"),
					Operator:    kv.OperatorEqual,
					Target:      kv.FieldMod,
					TargetUnion: kv.CompareTargetValue{ModRevision: intPtr(2)},
				},
			},
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("check"), Value: []byte("modRev_ok")}},
			},
			Failure: []kv.TxnOp{},
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
	defer s.backend.Close()
	_, err := e.ApplyWrite(kv.Command{Type: "UNKNOWN"})
	if err == nil {
		t.Error("expected error for unknown command type")
	}
}

func Test_EngineApplyTxn_ResultCount(t *testing.T) {
	e, s := newTestEngine()
	defer s.backend.Close()

	result, err := e.ApplyWrite(kv.Command{
		Type: kv.CmdTxn,
		Txn: &kv.TxnCommand{
			Success: []kv.TxnOp{
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("a"), Value: []byte("1")}},
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("b"), Value: []byte("2")}},
				{Type: kv.TxnOpPut, Put: &kv.PutCmd{Key: []byte("c"), Value: []byte("3")}},
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
