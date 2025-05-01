package dbutil_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func TestWithTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riverdbtest.TestTxPgx(ctx, t)
	driver := riverpgxv5.New(nil)

	err := dbutil.WithTx(ctx, driver.UnwrapExecutor(tx), func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		_, err := execTx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
}

func TestWithTxV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := riverdbtest.TestTxPgx(ctx, t)
	driver := riverpgxv5.New(nil)

	ret, err := dbutil.WithTxV(ctx, driver.UnwrapExecutor(tx), func(ctx context.Context, execTx riverdriver.ExecutorTx) (int, error) {
		_, err := execTx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		return 7, nil
	})
	require.NoError(t, err)
	require.Equal(t, 7, ret)
}
