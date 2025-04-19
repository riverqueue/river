package riverinternaltest

import (
	"context"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
)

// Implemented by `pgx.Tx` or `pgxpool.Pool`. Normally we'd use a similar type
// from `dbsqlc` or `dbutil`, but riverinternaltest is extremely low level and
// that would introduce a cyclic dependency. We could package as
// `riverinternaltest_test`, except that the test below uses internal variables
// like `dbPool`.
type Executor interface {
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
}

func TestTestTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	checkTestTable := func(executor Executor) error {
		_, err := executor.Exec(ctx, "SELECT * FROM test_tx_table")
		return err
	}

	// Test cleanups are invoked in the order of last added, first called. When
	// TestTx is called below it adds a cleanup, so we want to make sure that
	// this cleanup, which checks that the database remains pristine, is invoked
	// after the TestTx cleanup, so we add it first.
	t.Cleanup(func() {
		err := checkTestTable(riversharedtest.DBPool(ctx, t))
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
	})

	tx := TestTx(ctx, t)

	_, err := tx.Exec(ctx, "CREATE TABLE test_tx_table (id bigint)")
	require.NoError(t, err)

	err = checkTestTable(tx)
	require.NoError(t, err)
}
