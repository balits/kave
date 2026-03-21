package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
)

type KVService interface {
	Range(ctx context.Context, req api.KvRangeRequest) (*api.KvRangeResponse, error)
	Put(ctx context.Context, req api.KvPutRequest) (*api.KvPutResponse, error)
	Delete(ctx context.Context, req api.KvDeleteRequest) (*api.KvDeleteResponse, error)

	Ping() error
}

type kvSvc struct {
	store       *mvcc.KVStore
	proposeFunc util.ProposeFunc
	peerSvc     PeerService
	logger      *slog.Logger
}

func NewKVService(logger *slog.Logger, store *mvcc.KVStore, peerSvc PeerService, proposeFunc util.ProposeFunc) KVService {
	return &kvSvc{
		store:       store,
		proposeFunc: proposeFunc,
		peerSvc:     peerSvc,
		logger:      logger.With("component", "kv_service"),
	}
}

func (s *kvSvc) Range(ctx context.Context, req api.KvRangeRequest) (*api.KvRangeResponse, error) {
	s.logger.WithGroup("request").
		Info("Range request received",
			"key", req.Key,
			"end", req.End,
			"limit", req.Limit,
			"revision", req.Revision,
			"prefix", req.Prefix,
			"countOnly", req.CountOnly,
			"serializable", req.Serializable,
		)

	if !req.Serializable {
		if err := s.peerSvc.VerifyLeader(ctx); err != nil {
			return nil, fmt.Errorf("range failed: failed to verify leader: %v", err)
		}
	}

	if req.Prefix {
		req.End = kv.PrefixEnd(req.Key)
	}

	r := s.store.NewReader()
	entries, count, _, err := r.Range(req.Key, req.End, req.Revision, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("range failed: %v", err)
	}

	res := new(api.KvRangeResponse)
	res.Count = count
	if !req.CountOnly {
		res.Entries = entries
	}

	raftIndex, raftTerm := s.store.RaftMeta()
	currRev, _ := s.store.Revisions()

	res.Header = command.ResultHeader{
		Revision:  currRev.Main,
		RaftTerm:  raftTerm,
		RaftIndex: raftIndex,
		NodeID:    s.peerSvc.Me().NodeID,
	}
	return res, nil
}

func (s *kvSvc) Put(ctx context.Context, req api.KvPutRequest) (*api.KvPutResponse, error) {
	s.logger.WithGroup("request").
		Info("Put request received",
			"key", req.Key,
			"value", req.Value,
			"prevEntry", req.PrevEntry,
			"leaseID", req.LeaseID,
			"ignoreValue", req.IgnoreValue,
			"renewLease", req.IgnoreLease,
		)

	cmd := command.Command{
		Kind: command.KindPut,
		Put:  &req,
	}

	result, err := s.proposeFunc(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("put failed: %v", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("put failed: %v", result.Error)
	}
	if result.Put == nil {
		return nil, fmt.Errorf("put failed: %v", fsm.ErrNilApplyResult)
	}

	return &api.KvPutResponse{
		Header:                result.Header,
		KvPutResponseNoHeader: *result.Put,
	}, nil
}

func (s *kvSvc) Delete(ctx context.Context, req api.KvDeleteRequest) (*api.KvDeleteResponse, error) {
	s.logger.WithGroup("request").
		Info("Delete request received",
			"key", req.Key,
			"end", req.End,
			"prevEntries", req.PrevEntries,
		)

	cmd := command.Command{
		Kind:   command.KindDelete,
		Delete: &req,
	}

	result, err := s.proposeFunc(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("delete failed: %v", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("delete failed: %v", result.Error)
	}
	if result.Delete == nil {
		return nil, fmt.Errorf("delete failed: %v", fsm.ErrNilApplyResult)
	}

	return &api.KvDeleteResponse{
		Header:                   result.Header,
		KvDeleteResponseNoHeader: *result.Delete,
	}, nil
}

func (s *kvSvc) Ping() error {
	return s.store.Ping()
}
