package riverinternaltest

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
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
		err := checkTestTable(dbPool)
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

// Simulates a bunch of parallel processes starting a `TestTx` simultaneously.
// With the help of `go test -race`, should identify mutex/locking/parallel
// access problems if there are any.
//
// This test does NOT run in parallel on purpose because we want to be able to
// check access and set up on the `dbPool` global package variable which may be
// tainted if another test calls `TestTx` at the same time.
func TestTestTx_ConcurrentAccess(t *testing.T) { //nolint:paralleltest
	var (
		ctx = context.Background()
		wg  sync.WaitGroup
	)

	wg.Add(int(dbPoolMaxConns))

	// Before doing anything, zero out the pool because another test may have
	// initialized it already.
	dbPool = nil

	// Don't open more than maximum pool size transactions at once because that
	// would deadlock.
	for i := range dbPoolMaxConns {
		workerNum := i
		go func() {
			_ = TestTx(ctx, t)
			t.Logf("Opened transaction: %d", workerNum)
			wg.Done()
		}()
	}

	wg.Wait()
}
