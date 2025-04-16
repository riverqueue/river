package dbutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestWithTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riversharedtest.TestTx(ctx, t)
	driver := riverpgxv5.New(nil)

	err := WithTx(ctx, driver.UnwrapExecutor(tx), func(ctx context.Context, exec riverdriver.ExecutorTx) error {
		_, err := exec.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
}

func TestWithTxV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riversharedtest.TestTx(ctx, t)
	driver := riverpgxv5.New(nil)

	ret, err := WithTxV(ctx, driver.UnwrapExecutor(tx), func(ctx context.Context, exec riverdriver.ExecutorTx) (int, error) {
		_, err := exec.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return 7, nil
	})
	require.NoError(t, err)
	require.Equal(t, 7, ret)
}
