package dbutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func TestWithTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)
	driver := riverpgxv5.New(dbPool)

	err := WithTx(ctx, driver.GetExecutor(), func(ctx context.Context, exec riverdriver.ExecutorTx) error {
		_, err := exec.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
}

func TestWithTxV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)
	driver := riverpgxv5.New(dbPool)

	ret, err := WithTxV(ctx, driver.GetExecutor(), func(ctx context.Context, exec riverdriver.ExecutorTx) (int, error) {
		_, err := exec.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return 7, nil
	})
	require.NoError(t, err)
	require.Equal(t, 7, ret)
}
