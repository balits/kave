package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/util"
)

type KVService interface {
	// NOTE: since reads usually dont go through raft, the resulitng Header.NodeID will be "", the caller should set it themselves
	Range(ctx context.Context, cmd command.RangeCmd) (*command.Result, error)
	Put(ctx context.Context, subcmd command.PutCmd) (*command.Result, error)
	Delete(ctx context.Context, subcmd command.DeleteCmd) (*command.Result, error)

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

func (s *kvSvc) Range(ctx context.Context, cmd command.RangeCmd) (*command.Result, error) {
	s.logger.WithGroup("cmd").
		Debug("Range command received",
			"key", cmd.Key,
			"end", cmd.End,
			"revision", cmd.Revision,
			"limit", cmd.Limit,
			"countOnly", cmd.CountOnly,
			"prefix", cmd.Prefix,
		)
	// for now, only allow queries on leader
	if err := s.peerSvc.VerifyLeader(ctx); err != nil {
		return nil, fmt.Errorf("failed to verify leader: %v", err)
	}

	if cmd.Prefix {
		cmd.End = kv.PrefixEnd(cmd.Key)
	}

	r := s.store.NewReader()
	entries, count, _, err := r.Range(cmd.Key, cmd.End, cmd.Revision, cmd.Limit)
	if err != nil {
		return nil, fmt.Errorf("range failed: %v", err)
	}

	res := new(command.RangeResult)
	res.Count = count
	if !cmd.CountOnly {
		res.Entries = entries
	}

	raftIndex, raftTerm := s.store.RaftMeta()
	currRev, _ := s.store.Revisions()
	return &command.Result{
		Header: command.ResultHeader{
			Revision:  currRev.Main,
			RaftTerm:  raftTerm,
			RaftIndex: raftIndex,
			NodeID:    s.peerSvc.Me().NodeID,
		},
		RangeResult: res,
	}, nil
}

func (s *kvSvc) Put(ctx context.Context, subcmd command.PutCmd) (*command.Result, error) {
	s.logger.WithGroup("cmd").
		Debug("Put command received",
			"key", subcmd.Key,
			"value", subcmd.Value,
			"prevEntry", subcmd.PrevEntry,
			"leaseID", subcmd.LeaseID,
			"ignoreValue", subcmd.IgnoreValue,
			"renewLease", subcmd.RenewLease,
		)

	cmd := command.Command{
		Kind: command.KindPut,
		Put:  &subcmd,
	}

	result, err := s.proposeFunc(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("put failed: %v", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("put failed: %v", result.Error)
	}
	if result.PutResult == nil {
		return nil, fmt.Errorf("put failed: %v", fsm.ErrNilApplyResult)
	}
	return result, nil
}

func (s *kvSvc) Delete(ctx context.Context, subcmd command.DeleteCmd) (*command.Result, error) {
	s.logger.WithGroup("cmd").
		Debug("Delete command received",
			"key", subcmd.Key,
			"end", subcmd.End,
			"prevEntries", subcmd.PrevEntries,
		)

	cmd := command.Command{
		Kind:   command.KindDelete,
		Delete: &subcmd,
	}

	result, err := s.proposeFunc(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("delete failed: %v", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("delete failed: %v", result.Error)
	}
	if result.DeleteResult == nil {
		return nil, fmt.Errorf("delete failed: %v", fsm.ErrNilApplyResult)
	}
	return result, nil
}

func (s *kvSvc) Ping() error {
	return s.store.Ping()
}
