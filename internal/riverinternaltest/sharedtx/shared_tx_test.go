package sharedtx

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"weavelab.xyz/river/internal/riverinternaltest"
)

func TestSharedTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T) *SharedTx {
		t.Helper()

		return NewSharedTx(riverinternaltest.TestTx(ctx, t))
	}

	t.Run("SharedTxFunctions", func(t *testing.T) {
		t.Parallel()

		sharedTx := setup(t)

		_, err := sharedTx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		rows, err := sharedTx.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()

		row := sharedTx.QueryRow(ctx, "SELECT 1")
		var i int
		err = row.Scan(&i)
		require.NoError(t, err)

		require.Len(t, sharedTx.wait, 1)
	})

	t.Run("SharedSubTxFunctions", func(t *testing.T) {
		t.Parallel()

		sharedTx := setup(t)

		sharedSubTx, err := sharedTx.Begin(ctx)
		require.NoError(t, err)

		_, err = sharedSubTx.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		rows, err := sharedSubTx.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()

		row := sharedSubTx.QueryRow(ctx, "SELECT 1")
		var v int
		err = row.Scan(&v)
		require.NoError(t, err)
		require.Equal(t, 1, v)

		err = sharedSubTx.Commit(ctx)
		require.NoError(t, err)

		require.Len(t, sharedTx.wait, 1)
	})

	t.Run("TransactionCommitAndRollback", func(t *testing.T) {
		t.Parallel()

		sharedTx := setup(t)

		sharedSubTx, err := sharedTx.Begin(ctx)
		require.NoError(t, err)

		row := sharedSubTx.QueryRow(ctx, "SELECT 1")
		var v int
		err = row.Scan(&v)
		require.NoError(t, err)
		require.Equal(t, 1, v)

		err = sharedSubTx.Commit(ctx)
		require.NoError(t, err)

		// An additional rollback will return a tx closed error, but is safe.
		// The parent shared transaction will only be unlocked once.
		err = sharedSubTx.Rollback(ctx)
		require.ErrorIs(t, err, pgx.ErrTxClosed)

		require.Len(t, sharedTx.wait, 1)
	})

	t.Run("ConcurrentUse", func(t *testing.T) {
		t.Parallel()

		sharedTx := setup(t)

		const numIterations = 50
		errGroup, ctx := errgroup.WithContext(ctx)

		for i := 0; i < numIterations; i++ {
			errGroup.Go(func() error {
				sharedSubTx, err := sharedTx.Begin(ctx)
				require.NoError(t, err)

				row := sharedSubTx.QueryRow(ctx, "SELECT 1")
				var v int
				err = row.Scan(&v)
				require.NoError(t, err)
				require.Equal(t, 1, v)

				err = sharedSubTx.Commit(ctx)
				require.NoError(t, err)

				return nil
			})
		}

		for i := 0; i < numIterations; i++ {
			errGroup.Go(func() error {
				row := sharedTx.QueryRow(ctx, "SELECT 1")
				var v int
				err := row.Scan(&v)
				require.NoError(t, err)
				require.Equal(t, 1, v)

				return nil
			})
		}

		err := errGroup.Wait()
		require.NoError(t, err)

		require.Len(t, sharedTx.wait, 1)
	})
}
