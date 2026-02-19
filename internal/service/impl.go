package service

import (
	"context"
	"errors"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/common"
	"github.com/balits/thesis/internal/common/entry"
	"github.com/balits/thesis/internal/fsm"
	"github.com/balits/thesis/internal/store"
	"github.com/hashicorp/raft"
)

func New(raft *raft.Raft, store store.Storage) KVService {
	return &service{
		raft:  raft,
		store: store,
	}
}

type service struct {
	raft  *raft.Raft
	store store.Storage
}

func (s *service) GetStale(ctx context.Context, req GetRequest) (GetResponse, error) {
	select {
	case <-ctx.Done():
		return GetResponse{}, ctx.Err()
	default:
	}

	raw, err := s.store.Get([]byte(req.Key))
	if err != nil {
		return GetResponse{}, common.ErrStorageError
	}

	if raw != nil {
		return GetResponse{}, common.ErrKeyNotFound
	}

	entry, err := entry.Decode(raw)
	if err != nil {
		return GetResponse{}, err
	}

	return GetResponse{entry}, nil
}

func (s *service) GetConsistent(ctx context.Context, req GetRequest) (GetResponse, error) {
	if s.raft.State() != raft.Leader {
		return GetResponse{}, common.ErrNotLeader
	}

	cmd, err := command.Encode(command.Command{
		Type: command.CommandTypeGet,
		Key:  req.Key,
	})
	if err != nil {
		return GetResponse{}, err
	}

	fut := s.raft.Apply(cmd, 0)
	if err = waitFuture(ctx, fut); err != nil {
		return GetResponse{}, err
	}

	result := fut.Response().(fsm.AppyResult)
	if result.Error() != nil {
		return GetResponse{}, errors.Join(result.Error(), common.ErrStateMachineError)
	}

	entry := result.GetResult.Entry
	return GetResponse{entry}, nil
}

func (s *service) Set(ctx context.Context, req SetRequest) (SetResponse, error) {
	if s.raft.State() != raft.Leader {
		return SetResponse{}, common.ErrNotLeader
	}

	cmd, err := command.Encode(command.Command{
		Type:             command.CommandTypeSet,
		Key:              req.Key,
		Value:            req.Value,
		ExpectedRevision: req.ExpectedRevision,
	})
	if err != nil {
		return SetResponse{}, err
	}

	fut := s.raft.Apply(cmd, 0)
	if err = waitFuture(ctx, fut); err != nil {
		return SetResponse{}, err
	}

	result := fut.Response().(fsm.AppyResult)
	if result.Error() != nil {
		return SetResponse{}, errors.Join(result.Error(), common.ErrStateMachineError)
	}

	entry := result.SetResult.Entry
	return SetResponse{entry}, nil
}

func (s *service) Delete(ctx context.Context, req DeleteRequest) (DeleteResponse, error) {
	if s.raft.State() != raft.Leader {
		return DeleteResponse{}, common.ErrNotLeader
	}

	cmd, err := command.Encode(command.Command{
		Type:             command.CommandTypeDelete,
		Key:              req.Key,
		ExpectedRevision: req.ExpectedRevision,
	})
	if err != nil {
		return DeleteResponse{}, err
	}

	fut := s.raft.Apply(cmd, 0)
	if err = waitFuture(ctx, fut); err != nil {
		return DeleteResponse{}, err
	}

	result := fut.Response().(fsm.AppyResult)
	if result.Error() != nil {
		return DeleteResponse{}, errors.Join(result.Error(), common.ErrStateMachineError)
	}

	response := DeleteResponse{result.DeleteResult.Deleted, result.DeleteResult.PrevEntry}
	return response, nil
}

func (s *service) Txn(ctx context.Context, req TxnRequest) (TxnResponse, error) {
	if s.raft.State() != raft.Leader {
		return TxnResponse{}, common.ErrNotLeader
	}

	var cmds []command.Command
	for _, op := range req.ops {
		cmds = append(cmds, command.Command{
			Type:  command.CommandType(op.Type),
			Key:   op.Key,
			Value: op.Value,
		})
	}
	cmd, err := command.Encode(command.Command{
		Type:     command.CommandTypeBatch,
		BatchOps: cmds,
	})
	if err != nil {
		return TxnResponse{}, err
	}

	fut := s.raft.Apply(cmd, 0)
	if err = waitFuture(ctx, fut); err != nil {
		return TxnResponse{}, err
	}

	result := fut.Response().(fsm.AppyResult)
	if result.Error() != nil {
		return TxnResponse{false}, errors.Join(result.Error(), common.ErrStateMachineError)
	}

	return TxnResponse{result.BatchResult.Success}, nil
}

func waitFuture(ctx context.Context, fut raft.ApplyFuture) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fut.Error()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return errors.Join(err, common.ErrStateMachineError)
	}
}
