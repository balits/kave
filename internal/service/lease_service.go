package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
)

type LeaseService interface {
	Grant(ctx context.Context, req api.LeaseGrantRequest) (res *api.LeaseGrantResponse, err error)
	Revoke(ctx context.Context, req api.LeaseRevokeRequest) (res *api.LeaseRevokeResponse, err error)
	KeepAlive(ctx context.Context, req api.LeaseKeepAliveRequest) (res *api.LeaseKeepAliveResponse, err error)
}

func NewLeaseService(logger *slog.Logger, propose util.ProposeFunc) LeaseService {
	return &leaseSvc{
		propse: propose,
		logger: logger.With("component", "lease_service"),
	}
}

type leaseSvc struct {
	propse   util.ProposeFunc
	isLeader util.IsLeaderFunc
	logger   *slog.Logger
}

func (ls *leaseSvc) Grant(ctx context.Context, req api.LeaseGrantRequest) (*api.LeaseGrantResponse, error) {
	ls.logger.WithGroup("request").
		Debug("Recieved LeaseGrant request",
			"id", req.LeaseID,
			"ttl", req.TTL,
		)

	fut, err := ls.propse(command.Command{LeaseGrant: &req})
	returned, err := util.WaitApply(ctx, fut)
	if err != nil {
		return nil, fmt.Errorf("failed to grant lease: %w", err)
	}

	res, ok := returned.(command.Result)
	if !ok {
		return nil, fmt.Errorf("%w: %v", fsm.ErrStateMachineError, ErrUnexpectedResultType)
	}

	if res.Error != nil {
		return nil, fsm.ErrNilApplyResult
	}

	if res.LeaseGrantResult == nil {
		return nil, fmt.Errorf("%w: %v", fsm.ErrStateMachineError, ErrUnexpectedResultType)
	}

	return res.LeaseGrantResult, nil
}

func (ls *leaseSvc) Revoke(ctx context.Context, req api.LeaseRevokeRequest) (*api.LeaseRevokeResponse, error) {
	ls.logger.WithGroup("request").
		Debug("Recieved LeaseRevoke request",
			"id", req.LeaseID,
		)

	fut, err := ls.propse(command.Command{LeaseRevoke: &req})
	returned, err := util.WaitApply(ctx, fut)
	if err != nil {
		return nil, fmt.Errorf("failed to revoke lease: %w", err)
	}

	res, ok := returned.(command.Result)
	if !ok {
		return nil, fmt.Errorf("%w: %v", fsm.ErrStateMachineError, ErrUnexpectedResultType)
	}

	if res.Error != nil {
		return nil, fsm.ErrNilApplyResult
	}

	if res.LeaseRevokeResult == nil {
		return nil, fmt.Errorf("%w: %v", fsm.ErrStateMachineError, ErrUnexpectedResultType)
	}

	return res.LeaseRevokeResult, nil
}

func (ls *leaseSvc) KeepAlive(ctx context.Context, req api.LeaseKeepAliveRequest) (*api.LeaseKeepAliveResponse, error) {
	ls.logger.WithGroup("request").
		Debug("Recieved LeaseKeepAlive request",
			"id", req.LeaseID,
		)

	fut, err := ls.propse(command.Command{LeaseKeepAlive: &req})
	returned, err := util.WaitApply(ctx, fut)
	if err != nil {
		return nil, fmt.Errorf("failed to keep lease alive: %w", err)
	}

	res, ok := returned.(command.Result)
	if !ok {
		return nil, fmt.Errorf("%w: %v", fsm.ErrStateMachineError, ErrUnexpectedResultType)
	}

	if res.Error != nil {
		return nil, fsm.ErrNilApplyResult
	}

	if res.LeaseKeepAliveResult == nil {
		return nil, fmt.Errorf("%w: %v", fsm.ErrStateMachineError, ErrUnexpectedResultType)
	}

	return res.LeaseKeepAliveResult, nil
}
