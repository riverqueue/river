package dbutil

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/timeoututil"
)

// SafeIdentifier returns a safely quoted identifier (e.g. a table or schem
// name) for use in SQL queries. It wraps the identifier in quotes and escapes
// any internal quotes as appropriate.
func SafeIdentifier(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// RollbackWithoutCancel initiates a rollback, but one in which context is
// overridden with context.WithoutCancel so that the rollback can proceed even
// if a previous operation was cancelled. This decreases the chance that a
// transaction is accidentally left open and in an ambiguous state.
//
// A rollback error is returned the same way most driver rollbacks return an
// error, but given this is normally expected to be used in a defer statement,
// it's unusual for the error to be handled.
func RollbackWithoutCancel[TExec riverdriver.ExecutorTx](ctx context.Context, execTx TExec) error {
	ctxWithoutCancel := context.WithoutCancel(ctx)

	// It might not be the worst idea to log an unexpected error on rollback
	// here instead of returning it. I had this in place initially, but there's
	// a number of common errors that need to be ignored like "conn closed",
	// `pgx.ErrTxClosed`, or `sql.ErrTxDone`. These all turn out to be
	// driver-specific when this function is meant to be driver agnostic.
	//
	// It'd still be possible to make it happen, but we'd have to have a driver
	// function like `ShouldIgnoreRollbackError` that'd need a lot of plumbing
	// and it becomes questionable as to whether it's all worth it as Rollback
	// producing a non-standard error would be quite unusual.
	return timeoututil.WithTimeout(ctxWithoutCancel, 5*time.Second, "dbutil.RollbackWithoutCancel", execTx.Rollback)
}

// WithTx runs the given function in a transaction, reusing exec if it's already
// a transaction. Only a transaction started by WithTx is committed or rolled
// back; the owner of a reused transaction must roll it back on error.
// RIVER_USE_LEGACY_SUBTRANSACTIONS restores savepoints as described in WithTxV.
//
// Rollbacks use RollbackWithoutCancel to maximize the chance of a successful
// rollback even where an operation within the transaction timed out due to
// context timeout.
func WithTx[TExec riverdriver.Executor](ctx context.Context, exec TExec, innerFunc func(ctx context.Context, execTx riverdriver.ExecutorTx) error) error {
	_, err := WithTxV(ctx, exec, func(ctx context.Context, tx riverdriver.ExecutorTx) (struct{}, error) {
		return struct{}{}, innerFunc(ctx, tx)
	})
	return err
}

// WithTxV runs the given function in a transaction, allowing the return of a
// generic value. If exec is already a transaction, it is reused without starting
// a savepoint, committing, or rolling back. On error, the owner must roll back
// that transaction, including any partial writes made by innerFunc. Call Begin
// explicitly when an operation needs its own rollback boundary.
//
// As a temporary compatibility fallback, setting
// RIVER_USE_LEGACY_SUBTRANSACTIONS=1 (or true) disables transaction reuse and
// restores savepoints. This fallback is planned for removal in a future release.
//
// Rollbacks use RollbackWithoutCancel to maximize the chance of a successful
// rollback even where an operation within the transaction timed out due to
// context timeout.
func WithTxV[TExec riverdriver.Executor, T any](ctx context.Context, exec TExec, innerFunc func(ctx context.Context, execTx riverdriver.ExecutorTx) (T, error)) (T, error) {
	var defaultRes T

	legacySubtransactions := os.Getenv("RIVER_USE_LEGACY_SUBTRANSACTIONS")
	if tx, ok := any(exec).(riverdriver.ExecutorTx); ok && legacySubtransactions != "1" && legacySubtransactions != "true" {
		res, err := innerFunc(ctx, tx)
		if err != nil {
			return defaultRes, err
		}
		return res, nil
	}

	tx, err := exec.Begin(ctx)
	if err != nil {
		return defaultRes, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer RollbackWithoutCancel(ctx, tx) //nolint:errcheck

	res, err := innerFunc(ctx, tx)
	if err != nil {
		return defaultRes, err
	}

	if err := tx.Commit(ctx); err != nil {
		return defaultRes, fmt.Errorf("error committing transaction: %w", err)
	}

	return res, nil
}
