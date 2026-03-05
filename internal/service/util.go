package service

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"
)

// waitFuture megvárja egy raft future értékét, vagy a kontextus lejártát
// A visszaadott hiba vagy a future hibaértékét, vagy a kontextus lezárását jelzi
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
			return fmt.Errorf("raft future error: %v", err)
		}
		return nil
	}
}

// waitApply megvárja egy raft.ApplyFuture értékét, vagy a kontextus lejártát.
// A visszaadott hiba vagy belső fsm hibát, vagy a kontextus lezárását jelzi
func waitApply(ctx context.Context, fut raft.ApplyFuture) (any, error) {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fut.Error()
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
		return fut.Response(), nil
	}
}
