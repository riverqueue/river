package riverdrivertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/hashutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseExecutorTx[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	setup := func(ctx context.Context, t *testing.T) riverdriver.Executor {
		t.Helper()

		exec, _ := executorWithTx(ctx, t)
		return exec
	}

	t.Run("Begin", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicVisibility", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			tx, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })

			// Job visible in subtransaction, but not parent.
			{
				job := testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})
				_ = testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})

				_, err := tx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.NoError(t, err)

				require.NoError(t, tx.Rollback(ctx))

				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			}
		})

		t.Run("NestedTransactions", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			tx1, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(ctx) })

			// Job visible in tx1, but not top level executor.
			{
				job1 := testfactory.Job(ctx, t, tx1, &testfactory.JobOpts{})

				{
					tx2, err := tx1.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx2.Rollback(ctx) })

					// Job visible in tx2, but not top level executor.
					{
						job2 := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

						_, err := tx2.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
						require.NoError(t, err)

						require.NoError(t, tx2.Rollback(ctx))

						_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
						require.ErrorIs(t, err, rivertype.ErrNotFound)
					}

					_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
					require.NoError(t, err)
				}

				// Repeat the same subtransaction again.
				{
					tx2, err := tx1.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx2.Rollback(ctx) })

					job2 := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

					_, err = tx2.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
					require.NoError(t, err)

					require.NoError(t, tx2.Rollback(ctx))

					_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
					require.ErrorIs(t, err, rivertype.ErrNotFound)
				}

				_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
				require.NoError(t, err)

				require.NoError(t, tx1.Rollback(ctx))

				_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
				require.ErrorIs(t, err, rivertype.ErrNotFound)
			}
		})

		t.Run("RollbackAfterCommit", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			tx1, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(ctx) })

			tx2, err := tx1.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx2.Rollback(ctx) })

			job := testfactory.Job(ctx, t, tx2, &testfactory.JobOpts{})

			require.NoError(t, tx2.Commit(ctx))
			_ = tx2.Rollback(ctx) // "tx is closed" error generally returned, but don't require this

			// Despite rollback being called after commit, the job is still
			// visible from the outer transaction.
			_, err = tx1.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		t.Run("NoArgs", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			require.NoError(t, exec.Exec(ctx, "SELECT 1 + 2"))
		})

		t.Run("WithArgs", func(t *testing.T) {
			t.Parallel()

			exec := setup(ctx, t)

			require.NoError(t, exec.Exec(ctx, "SELECT $1 || $2", "foo", "bar"))
		})
	})

	t.Run("PGAdvisoryXactLock", func(t *testing.T) {
		t.Parallel()

		{
			driver, _ := driverWithSchema(ctx, t, nil)
			if driver.DatabaseName() == databaseNameSQLite {
				t.Logf("Skipping PGAdvisoryXactLock test for SQLite")
				return
			}
		}

		exec := setup(ctx, t)

		// It's possible for multiple versions of this test to be running at the
		// same time (from different drivers), so make sure the lock we're
		// acquiring per test is unique by using the complete test name. Also
		// add randomness in case a test is run multiple times with `-count`.
		lockHash := hashutil.NewAdvisoryLockHash(0)
		lockHash.Write([]byte(t.Name()))
		lockHash.Write([]byte(randutil.Hex(10)))
		key := lockHash.Key()

		// Tries to acquire the given lock from another test transaction and
		// returns true if the lock was acquired.
		tryAcquireLock := func(exec riverdriver.Executor) bool {
			var lockAcquired bool
			require.NoError(t, exec.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&lockAcquired))
			return lockAcquired
		}

		// Start a transaction to acquire the lock so we can later release the
		// lock by rolling back.
		execTx, err := exec.Begin(ctx)
		require.NoError(t, err)

		// Acquire the advisory lock on the main test transaction.
		_, err = execTx.PGAdvisoryXactLock(ctx, key)
		require.NoError(t, err)

		// Start another test transaction unrelated to the first.
		otherExec, _ := executorWithTx(ctx, t)

		// The other test transaction is unable to acquire the lock because the
		// first test transaction holds it.
		require.False(t, tryAcquireLock(otherExec))

		// Roll back the first test transaction to release the lock.
		require.NoError(t, execTx.Rollback(ctx))

		// The other test transaction can now acquire the lock.
		require.True(t, tryAcquireLock(otherExec))
	})

	t.Run("QueryRow", func(t *testing.T) {
		t.Parallel()

		exec := setup(ctx, t)

		var (
			field1   int
			field2   int
			field3   int
			fieldFoo string
		)

		err := exec.QueryRow(ctx, "SELECT 1, 2, 3, 'foo'").Scan(&field1, &field2, &field3, &fieldFoo)
		require.NoError(t, err)

		require.Equal(t, 1, field1)
		require.Equal(t, 2, field2)
		require.Equal(t, 3, field3)
		require.Equal(t, "foo", fieldFoo)
	})
}
