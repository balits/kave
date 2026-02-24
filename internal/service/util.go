package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/balits/kave/internal/common"
	"github.com/hashicorp/raft"
)

func waitFuture(ctx context.Context, fut raft.Future) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fut.Error()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			// saves us boilerplate on Writes in KVService
			if errors.Is(err, raft.ErrNotLeader) {
				return common.ErrNotLeader
			}
			return fmt.Errorf("%w: %v", common.ErrStateMachineError, err)
		}
		return nil
	}
}
