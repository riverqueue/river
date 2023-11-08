package dbutil

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river/internal/dbsqlc"
)

// Executor is an interface for a type that can begin a transaction and also
// perform all the operations needed to be used in conjunction with sqlc.
// Implemented by all of pgx's `pgxpool.Pool`, `pgx.Conn`, and `pgx.Tx`
// (transactions can start subtransactions).
type Executor interface {
	TxBeginner
	dbsqlc.DBTX
}

// TxBeginner is an interface to a type that can begin a transaction, like a pgx
// connection pool, connection, or transaction (the latter would begin a
// subtransaction).
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// WithTx starts and commits a transaction around the given function, allowing
// the return of a generic value.
func WithTx(ctx context.Context, txBeginner TxBeginner, innerFunc func(ctx context.Context, tx pgx.Tx) error) error {
	_, err := WithTxV(ctx, txBeginner, func(ctx context.Context, tx pgx.Tx) (struct{}, error) {
		return struct{}{}, innerFunc(ctx, tx)
	})
	return err
}

// WithTxV starts and commits a transaction around the given function, allowing
// the return of a generic value.
func WithTxV[T any](ctx context.Context, txBeginner TxBeginner, innerFunc func(ctx context.Context, tx pgx.Tx) (T, error)) (T, error) {
	var defaultRes T

	tx, err := txBeginner.Begin(ctx)
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
