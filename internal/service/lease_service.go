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

	result, err := ls.propse(ctx, command.Command{LeaseGrant: &req})
	if err != nil {
		return nil, fmt.Errorf("grant failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("grant failed: %w", result.Error)
	}
	if result.LeaseGrantResult == nil {
		return nil, fmt.Errorf("grant failed: %w", fsm.ErrNilApplyResult)
	}

	return result.LeaseGrantResult, nil
}

func (ls *leaseSvc) Revoke(ctx context.Context, req api.LeaseRevokeRequest) (*api.LeaseRevokeResponse, error) {
	ls.logger.WithGroup("request").
		Debug("Recieved LeaseRevoke request",
			"id", req.LeaseID,
		)

	result, err := ls.propse(ctx, command.Command{LeaseRevoke: &req})
	if err != nil {
		return nil, fmt.Errorf("revoke failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("revoke failed: %w", result.Error)
	}
	if result.LeaseRevokeResult == nil {
		return nil, fmt.Errorf("revoke failed: %w", fsm.ErrNilApplyResult)
	}

	return result.LeaseRevokeResult, nil
}

func (ls *leaseSvc) KeepAlive(ctx context.Context, req api.LeaseKeepAliveRequest) (*api.LeaseKeepAliveResponse, error) {
	ls.logger.WithGroup("request").
		Debug("Recieved LeaseKeepAlive request",
			"id", req.LeaseID,
		)

	result, err := ls.propse(ctx, command.Command{LeaseKeepAlive: &req})
	if err != nil {
		return nil, fmt.Errorf("keep alive failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("keep alive failed: %w", result.Error)
	}
	if result.LeaseKeepAliveResult == nil {
		return nil, fmt.Errorf("keep alive failed: %w", fsm.ErrNilApplyResult)
	}

	return result.LeaseKeepAliveResult, nil
}
