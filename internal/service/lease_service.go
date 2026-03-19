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
	Lookup(ctx context.Context, req api.LeaseLookupRequest) (res *api.LeaseLookupResponse, err error)
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
		Debug("Recieved Grant request",
			"id", req.LeaseID,
			"ttl", req.TTL,
		)

	result, err := ls.propse(ctx, command.Command{
		Kind:       command.KindLeaseGrant,
		LeaseGrant: &req,
	})
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
		Debug("Recieved Revoke request",
			"id", req.LeaseID,
		)

	result, err := ls.propse(ctx, command.Command{
		Kind:        command.KindLeaseRevoke,
		LeaseRevoke: &req,
	})
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
		Debug("Recieved KeepAlive request",
			"id", req.LeaseID,
		)

	result, err := ls.propse(ctx, command.Command{
		Kind:           command.KindLeaseKeepAlive,
		LeaseKeepAlive: &req,
	})
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

func (ls *leaseSvc) Lookup(ctx context.Context, req api.LeaseLookupRequest) (res *api.LeaseLookupResponse, err error) {
	ls.logger.WithGroup("request").
		Debug("Recieved Lookup request",
			"id", req.LeaseID,
		)

	result, err := ls.propse(ctx, command.Command{
		Kind:        command.KindLeaseLookup,
		LeaseLookup: &req,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup failed: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("lookup failed: %w", result.Error)
	}
	if result.LeaseLookupResult == nil {
		return nil, fmt.Errorf("lookup failed: %w", fsm.ErrNilApplyResult)
	}

	return result.LeaseLookupResult, nil
}
