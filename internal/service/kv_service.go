package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/hashicorp/raft"
)

var errKvService = errors.New("kv service error")

// TODO: move cmd.Check() to transport layer
// TODO: return kv.Result instead of subresults <- fill out raft header fields inside fsm
type KVService interface {
	// NOTE: since reads usually dont go through raft, the resulitng Header.NodeID will be "", the caller should set it themselves
	Range(ctx context.Context, cmd kv.RangeCmd) (*kv.Result, error)
	Put(ctx context.Context, subcmd kv.PutCmd) (*kv.Result, error)
	Delete(ctx context.Context, subcmd kv.DeleteCmd) (*kv.Result, error)
}

type raftKvService struct {
	raft    *raft.Raft
	store   *mvcc.KVStore
	peerSvc PeerService
	logger  *slog.Logger
}

func NewKVService(logger *slog.Logger, r *raft.Raft, store *mvcc.KVStore, peerSvc PeerService) KVService {
	return &raftKvService{
		raft:    r,
		store:   store,
		peerSvc: peerSvc,
		logger:  logger.With("component", "kv_service"),
	}
}

func (s *raftKvService) Range(ctx context.Context, cmd kv.RangeCmd) (*kv.Result, error) {
	s.logger.WithGroup("cmd").
		Debug("Range request received",
			"key", cmd.Key,
			"end", cmd.End,
			"revision", cmd.Revision,
			"limit", cmd.Limit,
			"countOnly", cmd.CountOnly,
			"prefix", cmd.Prefix,
		)
	// for now, only allow queries on leader
	if err := waitFuture(ctx, s.raft.VerifyLeader()); err != nil {
		return nil, fmt.Errorf("%w: failed to verify leader: %v", errKvService, err)
	}

	if cmd.Prefix {
		cmd.End = kv.PrefixEnd(cmd.Key)
	}

	r := s.store.NewReader()
	entries, count, _, err := r.Range(cmd.Key, cmd.End, cmd.Revision, cmd.Limit)
	if err != nil {
		return nil, fmt.Errorf("%w: range failed: %v", errKvService, err)
	}

	res := new(kv.RangeResult)
	res.Count = count
	if !cmd.CountOnly {
		res.Entries = entries
	}

	raftIndex, raftTerm := s.store.RaftMeta()
	currRev, _ := s.store.Revisions()
	return &kv.Result{
		Header: kv.ResultHeader{
			Revision:  currRev.Main,
			RaftTerm:  raftTerm,
			RaftIndex: raftIndex,
			NodeID:    s.peerSvc.Me().NodeID,
		},
		Range: res,
	}, nil
}

func (s *raftKvService) Put(ctx context.Context, subcmd kv.PutCmd) (*kv.Result, error) {
	s.logger.WithGroup("cmd").
		Debug("Put request received",
			"key", subcmd.Key,
			"value", subcmd.Value,
			"prevEntry", subcmd.PrevEntry,
			"leaseID", subcmd.LeaseID,
			"ignoreValue", subcmd.IgnoreValue,
			"renewLease", subcmd.RenewLease,
		)

	cmd := kv.Command{
		Type: kv.CmdPut,
		Put:  &subcmd,
	}

	cmdBytes, err := kv.EncodeCommand(cmd)
	if err != nil {
		return nil, err
	}

	fut := s.raft.Apply(cmdBytes, 0)
	returned, err := waitApply(ctx, fut)
	if err != nil {
		return nil, fmt.Errorf("%w: apply failed: %v", errKvService, err)
	}

	result, ok := returned.(kv.Result)
	if !ok {
		return nil, fmt.Errorf("%w: %v: unexpected result type", errKvService, fsm.ErrStateMachineError)
	}

	if result.Error != nil {
		return nil, fmt.Errorf("%w: %v", errKvService, result.Error)
	}

	if result.Put == nil {
		return nil, fmt.Errorf("%w: nil result from fsm", errKvService)
	}
	return &result, nil
}

func (s *raftKvService) Delete(ctx context.Context, subcmd kv.DeleteCmd) (*kv.Result, error) {
	s.logger.WithGroup("cmd").
		Debug("Delete request received",
			"key", subcmd.Key,
			"end", subcmd.End,
			"prevEntries", subcmd.PrevEntries,
		)

	cmd := kv.Command{
		Type:   kv.CmdDelete,
		Delete: &subcmd,
	}
	cmdBytes, err := kv.EncodeCommand(cmd)
	if err != nil {
		return nil, err
	}

	fut := s.raft.Apply(cmdBytes, 0)
	returned, err := waitApply(ctx, fut)
	if err != nil {
		return nil, fmt.Errorf("%w: apply failed: %v", errKvService, err)
	}

	result, ok := returned.(kv.Result)
	if !ok {
		return nil, fmt.Errorf("%w: %v: unexpected result type", errKvService, fsm.ErrStateMachineError)
	}

	if result.Error != nil {
		return nil, fmt.Errorf("%w: %v", errKvService, result.Error)
	}

	if result.Delete == nil {
		return nil, fmt.Errorf("%w: nil result from fsm", errKvService)
	}
	return &result, nil
}
