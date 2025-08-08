package dbutil

import (
	"context"
	"fmt"

	"github.com/riverqueue/river/riverdriver"
)

// WithTx starts and commits a transaction on a driver executor around
// the given function, allowing the return of a generic value.
func WithTx[TExec riverdriver.Executor](ctx context.Context, exec TExec, innerFunc func(ctx context.Context, execTx riverdriver.ExecutorTx) error) error {
	_, err := WithTxV(ctx, exec, func(ctx context.Context, tx riverdriver.ExecutorTx) (struct{}, error) {
		return struct{}{}, innerFunc(ctx, tx)
	})
	return err
}

// WithTxV starts and commits a transaction on a driver executor around
// the given function, allowing the return of a generic value.
func WithTxV[TExec riverdriver.Executor, T any](ctx context.Context, exec TExec, innerFunc func(ctx context.Context, execTx riverdriver.ExecutorTx) (T, error)) (T, error) {
	var defaultRes T

	tx, err := exec.Begin(ctx)
	if err != nil {
		return defaultRes, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	res, err := innerFunc(ctx, tx)
	if err != nil {
		return defaultRes, err
	}

	if err := tx.Commit(ctx); err != nil {
		return defaultRes, fmt.Errorf("error committing transaction: %w", err)
	}

	return res, nil
}
