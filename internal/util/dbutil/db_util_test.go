package dbutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"weavelab.xyz/river/internal/riverinternaltest"
	"weavelab.xyz/river/riverdriver"
	"weavelab.xyz/river/riverdriver/riverpgxv5"
)

func TestWithTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)

	err := WithTx(ctx, dbPool, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
}

func TestWithTxV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)

	ret, err := WithTxV(ctx, dbPool, func(ctx context.Context, tx pgx.Tx) (int, error) {
		_, err := tx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return 7, nil
	})
	require.NoError(t, err)
	require.Equal(t, 7, ret)
}

func TestWithExecutorTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)
	driver := riverpgxv5.New(dbPool)

	err := WithExecutorTx(ctx, driver.GetExecutor(), func(ctx context.Context, tx riverdriver.ExecutorTx) error {
		_, err := tx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
}

func TestWithExecutorTxV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)
	driver := riverpgxv5.New(dbPool)

	ret, err := WithExecutorTxV(ctx, driver.GetExecutor(), func(ctx context.Context, tx riverdriver.ExecutorTx) (int, error) {
		_, err := tx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return 7, nil
	})
	require.NoError(t, err)
	require.Equal(t, 7, ret)
}
