package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
)

type OTService interface {
	Init(ctx context.Context, req api.OTInitRequest) (*api.OTInitResponse, error)
	Transfer(ctx context.Context, req api.OTTransferRequest) (*api.OTTransferResponse, error)
	WriteAll(ctx context.Context, req api.OTWriteAllRequest) (*api.OTWriteAllResponse, error)
}

type otSvc struct {
	me       peer.Peer
	store    mvcc.StoreMetaReader
	otReader ot.ReadOnlyOT
	raftSvc  RaftService
	propose  util.ProposeFunc
	logger   *slog.Logger
}

func NewOTService(logger *slog.Logger, me peer.Peer, store mvcc.StoreMetaReader, otReader ot.ReadOnlyOT, raftSvc RaftService, propose util.ProposeFunc) OTService {
	return &otSvc{
		me:       me,
		store:    store,
		otReader: otReader,
		raftSvc:  raftSvc,
		propose:  propose,
		logger:   logger.With("component", "ot_service"),
	}
}

func (s *otSvc) Init(ctx context.Context, _ api.OTInitRequest) (*api.OTInitResponse, error) {
	s.logger.Info("Init request received")

	// since Init doesnt go through raft,
	// we dont gotta check before "proposing"
	a, token, err := s.otReader.Init()
	if err != nil {
		return nil, fmt.Errorf("init failed: %w", err)
	}

	currRev, compactedRev, raftIndex, raftTerm := s.store.Meta()
	header := api.ResponseHeader{
		Revision:     currRev.Main,
		CompactedRev: compactedRev,
		RaftTerm:     raftTerm,
		RaftIndex:    raftIndex,
		NodeID:       s.me.NodeID,
	}

	res := api.OTInitResponseNoHeader{
		PointA: a,
		Token:  token,
	}

	return &api.OTInitResponse{
		Header:                 header,
		OTInitResponseNoHeader: res,
	}, nil
}

func (s *otSvc) Transfer(ctx context.Context, req api.OTTransferRequest) (*api.OTTransferResponse, error) {
	s.logger.Info("Transfer request received")

	if !req.Serializable {
		if err := s.raftSvc.VerifyLeader(ctx); err != nil {
			return nil, fmt.Errorf("transfer failed: failed to verify leader: %v", err)
		}
	}

	// since Transfer doesnt go through raft,
	// we dont gotta check before "proposing"
	ciphertexts, err := s.otReader.Transfer(req.Token, req.PointB)
	if err != nil {
		return nil, fmt.Errorf("transfer failed: %w", err)
	}

	currRev, compactedRev, raftIndex, raftTerm := s.store.Meta()
	header := api.ResponseHeader{
		Revision:     currRev.Main,
		CompactedRev: compactedRev,
		RaftTerm:     raftTerm,
		RaftIndex:    raftIndex,
		NodeID:       s.me.NodeID,
	}

	res := api.OTTransferResponseNoHeader{
		Ciphertexts: ciphertexts,
	}

	return &api.OTTransferResponse{
		Header:                     header,
		OTTransferResponseNoHeader: res,
	}, nil
}

func (s *otSvc) WriteAll(ctx context.Context, req api.OTWriteAllRequest) (*api.OTWriteAllResponse, error) {
	s.logger.Info("WriteAll request received")

	// since WriteALl does go through raft,
	// its nicer to check before hand, even though
	// ApplyWriteAll checks blob again
	if err := s.otReader.CheckBlob(req.Blob); err != nil {
		return nil, fmt.Errorf("write all failed: %w", err)
	}

	cmd := command.Command{
		Kind:       command.KindOTWriteAll,
		OTWriteAll: &req,
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("write all failed: %v", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("write all failed: %v", result.Error)
	}
	if result.OtWriteAll == nil {
		return nil, fmt.Errorf("write all failed: %v", fsm.ErrNilApplyResult)
	}

	return &api.OTWriteAllResponse{
		Header:                     result.Header,
		OTWriteAllResponseNoHeader: *result.OtWriteAll,
	}, nil
}
