package dbutil_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/dbutil"
)

func TestRollbackCancelOverride(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		return &testBundle{
			driver: riverpgxv5.New(nil),
			tx:     riverdbtest.TestTxPgx(ctx, t),
		}
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		dbutil.RollbackWithoutCancel(ctx, bundle.driver.UnwrapExecutor(bundle.tx))
	})

	t.Run("WithCancelledContext", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		ctx, cancel := context.WithCancel(ctx)
		cancel()

		dbutil.RollbackWithoutCancel(ctx, bundle.driver.UnwrapExecutor(bundle.tx))
	})

	t.Run("RollbackError", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		execTx := &executorTxWithRollbackError{
			ExecutorTx: bundle.driver.UnwrapExecutor(bundle.tx),
		}

		err := dbutil.RollbackWithoutCancel(ctx, execTx)
		require.EqualError(t, err, "rollback error")
	})
}

type executorTxWithRollbackError struct {
	riverdriver.ExecutorTx
}

func (e *executorTxWithRollbackError) Rollback(ctx context.Context) error {
	return errors.New("rollback error")
}

func TestWithTx(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		tx     = riverdbtest.TestTxPgx(ctx, t)
		driver = riverpgxv5.New(nil)
	)

	err := dbutil.WithTx(ctx, driver.UnwrapExecutor(tx), func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		require.NoError(t, execTx.Exec(ctx, "SELECT 1"))
		return nil
	})
	require.NoError(t, err)
}

func TestWithTxV(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		tx     = riverdbtest.TestTxPgx(ctx, t)
		driver = riverpgxv5.New(nil)
	)

	ret, err := dbutil.WithTxV(ctx, driver.UnwrapExecutor(tx), func(ctx context.Context, execTx riverdriver.ExecutorTx) (int, error) {
		require.NoError(t, execTx.Exec(ctx, "SELECT 1"))
		return 7, nil
	})
	require.NoError(t, err)
	require.Equal(t, 7, ret)
}
