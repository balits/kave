package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types/api"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type testOTService struct {
	OTService
	t   *testing.T
	ctx context.Context
	om  *ot.OTManager
}

func newTestOTService(t *testing.T) (*testOTService, ot.MockOTClient) {
	logger := slog.Default()
	me := peer.TestPeer()
	reg := metrics.InitTestPrometheus()
	be := backend.New(reg, logger, storage.Options{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	kvstore := mvcc.NewKvStore(reg, logger, be)
	lm := lease.NewManager(reg, logger, kvstore, be)
	t.Cleanup(func() { be.Close() })

	om, err := ot.NewOTManager(reg, logger, be, ot.DefaultOptions)
	require.NoError(t, err)
	require.NoError(t, om.ApplyGenerateClusterKey(ot.RandomKey256()), "failed to generate cluster key")

	f := fsm.New(logger, me, kvstore, lm, om)

	var logIndex atomic.Uint64
	propose := func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		bs, err := command.Encode(cmd)
		if err != nil {
			return nil, err
		}
		idx := logIndex.Add(1)
		res := f.Apply(&raft.Log{
			Index: idx,
			Data:  bs,
			Term:  1,
			Type:  raft.LogCommand,
		})
		result, ok := res.(command.Result)
		if !ok {
			return nil, fmt.Errorf("unexpected result type from FSM")
		}
		return &result, nil
	}

	peerSvc := &MockRaftService{
		Me_:     me,
		Leader_: me,
		State_:  raft.Leader,
	}
	svc := NewOTService(logger, me, kvstore, om, peerSvc, propose)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	return &testOTService{
		OTService: svc,
		t:         t,
		ctx:       ctx,
		om:        om,
	}, ot.MockOTClient{T: t}
}

func makeTestBlob(slotCount, slotSize int) []byte {
	blob := make([]byte, slotCount*slotSize)
	for i := range slotCount {
		offset := i * slotSize
		blob[offset] = byte(i)
		for j := 1; j < slotSize; j++ {
			blob[offset+j] = byte(i + 1 + j)
		}
	}
	return blob
}

func (ts *testOTService) mustInit() *api.OTInitResponse {
	ts.t.Helper()
	res, err := ts.Init(ts.ctx, api.OTInitRequest{})
	require.NoError(ts.t, err)
	require.NotNil(ts.t, res)
	return res
}

func (ts *testOTService) mustWriteAll(blob []byte) *api.OTWriteAllResponse {
	ts.t.Helper()
	res, err := ts.WriteAll(ts.ctx, api.OTWriteAllRequest{Blob: blob})
	require.NoError(ts.t, err, "WriteAll failed")
	require.NotNil(ts.t, res)
	return res
}

func Test_OTService_Init_ReturnsPointAAndToken(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	res := ts.mustInit()

	require.Len(t, res.Token, ot.TokenSize)
	require.Len(t, res.PointA, 32, "Ristretto255 point is 32 bytes")
	require.NotEmpty(t, res.Header.NodeID)
}

func Test_OTService_Init_UniquePerCall(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	r1 := ts.mustInit()
	r2 := ts.mustInit()
	require.NotEqual(t, r1.Token, r2.Token)
}

func Test_OTService_WriteAll_OK(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	res := ts.mustWriteAll(blob)
	require.NotNil(t, res)
	require.NotZero(t, res.Header.RaftIndex)
}

func Test_OTService_WriteAll_RejectsNilBlob(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	_, err := ts.WriteAll(ts.ctx, api.OTWriteAllRequest{Blob: nil})
	require.Error(t, err)
}

func Test_OTService_WriteAll_RejectsWrongSizeBlob(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	_, err := ts.WriteAll(ts.ctx, api.OTWriteAllRequest{Blob: []byte("too small")})
	require.Error(t, err)
}

func Test_OTService_Transfer_ReturnsNSlotCiphertexts(t *testing.T) {
	t.Parallel()
	ts, c := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	ts.mustWriteAll(blob)

	initRes := ts.mustInit()
	choice := 0
	pointB, _ := c.BlindedChoice(initRes.PointA, choice)

	transferRes, err := ts.Transfer(ts.ctx, api.OTTransferRequest{
		Token:  initRes.Token,
		PointB: pointB,
	})
	require.NoError(t, err)
	require.Len(t, transferRes.Ciphertexts, ot.DefaultSlotCount)
}

func Test_OTService_Transfer_NoBlobWritten_Fails(t *testing.T) {
	t.Parallel()
	ts, c := newTestOTService(t)
	initRes := ts.mustInit()

	pointB, _ := c.BlindedChoice(initRes.PointA, 0)
	_, err := ts.Transfer(ts.ctx, api.OTTransferRequest{
		Token:  initRes.Token,
		PointB: pointB,
	})
	require.Error(t, err)
}

func Test_OTService_Transfer_InvalidPointB_Fails(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	ts.mustWriteAll(blob)

	initRes := ts.mustInit()
	_, err := ts.Transfer(ts.ctx, api.OTTransferRequest{
		Token:  initRes.Token,
		PointB: []byte("bad"),
	})
	require.Error(t, err)
}

func Test_OTService_Transfer_TamperedToken_Fails(t *testing.T) {
	t.Parallel()
	ts, c := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	ts.mustWriteAll(blob)

	initRes := ts.mustInit()
	pointB, _ := c.BlindedChoice(initRes.PointA, 0)

	tampered := make([]byte, len(initRes.Token))
	copy(tampered, initRes.Token)
	tampered[14] ^= 0xFF

	_, err := ts.Transfer(ts.ctx, api.OTTransferRequest{
		Token:  tampered,
		PointB: pointB,
	})
	require.Error(t, err)
}

// E2E: Init → WriteAll → Transfer → client.decrypt

func Test_OTService_E2E_ChosenSlotDecrypts(t *testing.T) {
	t.Parallel()
	ts, c := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	ts.mustWriteAll(blob)

	initRes := ts.mustInit()
	choice := 5
	pointB, b := c.BlindedChoice(initRes.PointA, choice)

	transferRes, err := ts.Transfer(ts.ctx, api.OTTransferRequest{
		Token:  initRes.Token,
		PointB: pointB,
	})
	require.NoError(t, err)
	require.Len(t, transferRes.Ciphertexts, ot.DefaultSlotCount)

	expected := blob[choice*ot.DefaultSlotSize : (choice+1)*ot.DefaultSlotSize]
	got, err := c.TryDecrypt(initRes.PointA, b, transferRes.Ciphertexts[choice])
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func Test_OTService_E2E_NonChosenSlotsFail(t *testing.T) {
	t.Parallel()
	ts, c := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	ts.mustWriteAll(blob)

	initRes := ts.mustInit()
	choice := 3
	pointB, b := c.BlindedChoice(initRes.PointA, choice)

	transferRes, err := ts.Transfer(ts.ctx, api.OTTransferRequest{
		Token:  initRes.Token,
		PointB: pointB,
	})
	require.NoError(t, err)

	for i, ct := range transferRes.Ciphertexts {
		if i == choice {
			continue
		}
		_, err := c.TryDecrypt(initRes.PointA, b, ct)
		require.Error(t, err, "slot %d should NOT decrypt with choice=%d key", i, choice)
	}
}

func Test_OTService_Init_HeaderContainsNodeID(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	res := ts.mustInit()
	require.Equal(t, peer.TestPeer().NodeID, res.Header.NodeID)
}

func Test_OTService_WriteAll_HeaderHasRaftMeta(t *testing.T) {
	t.Parallel()
	ts, _ := newTestOTService(t)
	blob := makeTestBlob(ot.DefaultSlotCount, ot.DefaultSlotSize)
	res := ts.mustWriteAll(blob)

	require.NotZero(t, res.Header.RaftIndex)
	require.NotZero(t, res.Header.RaftTerm)
	require.Equal(t, peer.TestPeer().NodeID, res.Header.NodeID)
}
