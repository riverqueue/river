package riverdrivertest

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/dbutil"
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

		t.Run("CancelledBeginLeavesPoolUsable", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			exec := driver.GetExecutor()

			// Race cancellation against BEGIN. A driver may start the
			// transaction but return an error if cancellation arrives just
			// afterwards. Subsequent transactions must still work.
			for range 100 {
				beginCtx, cancel := context.WithCancel(ctx)
				var cancelGroup sync.WaitGroup
				cancelGroup.Go(cancel)
				tx, err := exec.Begin(beginCtx)
				cancelGroup.Wait()
				if err == nil {
					_ = tx.Rollback(ctx)
				}

				tx, err = exec.Begin(ctx)
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))
			}
		})

		t.Run("CancelledSQLiteTransactionReleasesConnection", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			if driver.DatabaseName() != riverdriver.DatabaseNameSQLite {
				t.Skip("SQLite pools use one connection and database/sql rolls back automatically on cancellation")
			}
			exec := driver.GetExecutor()

			beginCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			tx, err := exec.Begin(beginCtx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })
			cancel()

			// No explicit rollback: cancellation alone must return the only
			// connection to the pool after database/sql rolls back.
			nextCtx, nextCancel := context.WithTimeout(ctx, 5*time.Second)
			defer nextCancel()
			nextTx, err := exec.Begin(nextCtx)
			require.NoError(t, err)
			require.NoError(t, nextTx.Rollback(ctx))
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
			if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
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

	t.Run("WithTx", func(t *testing.T) {
		t.Parallel()

		legacySubtransactions := os.Getenv("RIVER_USE_LEGACY_SUBTRANSACTIONS") == "1" || os.Getenv("RIVER_USE_LEGACY_SUBTRANSACTIONS") == "true"
		for _, name := range []string{"BorrowedDatabaseError", "BorrowedError", "BorrowedSuccess", "OwnedError", "OwnedSuccess"} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				driver, schema := driverWithSchema(ctx, t, nil)
				exec := driver.GetExecutor()
				var borrowed riverdriver.ExecutorTx
				var priorJob *rivertype.JobRow
				if name == "BorrowedDatabaseError" || name == "BorrowedError" || name == "BorrowedSuccess" {
					var err error
					borrowed, err = exec.Begin(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = borrowed.Rollback(ctx) })
					exec = borrowed
					priorJob = testfactory.Job(ctx, t, borrowed, &testfactory.JobOpts{Schema: schema})
				}

				var job *rivertype.JobRow
				var innerErr error
				if name == "BorrowedError" || name == "OwnedError" {
					innerErr = errors.New("error after writing")
				}
				result, err := dbutil.WithTxV(ctx, exec, func(ctx context.Context, tx riverdriver.ExecutorTx) (int64, error) {
					if borrowed != nil {
						if legacySubtransactions {
							require.NotSame(t, borrowed, tx, "legacy mode creates a savepoint")
						} else {
							require.Same(t, borrowed, tx, "reuse the caller's transaction without a savepoint")
						}
					}
					job = testfactory.Job(ctx, t, tx, &testfactory.JobOpts{Schema: schema})
					if name == "BorrowedDatabaseError" {
						innerErr = tx.Exec(ctx, "SELECT * FROM river_nonexistent_table")
						require.Error(t, innerErr)
					}
					return job.ID, innerErr
				})
				if innerErr != nil {
					require.ErrorIs(t, err, innerErr)
					require.Zero(t, result)
				} else {
					require.NoError(t, err)
					require.Equal(t, job.ID, result)
				}

				if borrowed != nil {
					_, err := borrowed.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
					aborted := name == "BorrowedDatabaseError" && !legacySubtransactions && driver.DatabaseName() == riverdriver.DatabaseNamePostgres
					switch {
					case aborted:
						require.Error(t, err)
					case legacySubtransactions && innerErr != nil:
						require.ErrorIs(t, err, rivertype.ErrNotFound, "the savepoint rolls back partial writes")
					default:
						require.NoError(t, err)
					}
					if !aborted {
						_, err = borrowed.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: priorJob.ID, Schema: schema})
						require.NoError(t, err, "earlier writes remain in the caller's transaction")
					}
					if legacySubtransactions && innerErr != nil {
						require.NoError(t, borrowed.Commit(ctx), "the caller can commit earlier writes after an error")
						_, err = driver.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: priorJob.ID, Schema: schema})
						require.NoError(t, err)
					} else {
						require.NoError(t, borrowed.Rollback(ctx))
					}
				}
				_, err = driver.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
				if borrowed != nil || innerErr != nil {
					require.ErrorIs(t, err, rivertype.ErrNotFound)
				} else {
					require.NoError(t, err)
				}
			})
		}

		t.Run("TransactionIDs", func(t *testing.T) {
			t.Parallel()

			// Inspect the actual writes as well as executor identity: nested
			// helpers or driver internals must not allocate subtransaction IDs
			// unless the legacy fallback is explicitly enabled.
			driver, schema := driverWithSchema(ctx, t, nil)
			if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
				t.Skip("uses Postgres tuple transaction IDs to detect hidden subtransactions")
			}
			tx, err := driver.GetExecutor().Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })

			// Include a write directly in the caller's transaction so that
			// even one savepoint encompassing all helper writes is detected.
			_ = testfactory.Job(ctx, t, tx, &testfactory.JobOpts{Schema: schema})
			const numJobs = 80 // Exceeds Postgres's cached subtransaction ID limit.
			for range numJobs {
				require.NoError(t, dbutil.WithTx(ctx, tx, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
					return dbutil.WithTx(ctx, execTx, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
						_ = testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{Schema: schema})
						return nil
					})
				}))
			}

			var numRows, numTransactions int
			require.NoError(t, tx.QueryRow(ctx, "SELECT count(*), count(DISTINCT xmin::text) FROM "+dbutil.SafeIdentifier(schema)+".river_job").Scan(&numRows, &numTransactions))
			require.Equal(t, numJobs+1, numRows)
			if legacySubtransactions {
				require.Equal(t, numJobs+1, numTransactions, "legacy mode allocates a subtransaction ID for each helper write")
			} else {
				require.Equal(t, 1, numTransactions, "all writes must use the caller's transaction ID by default")
			}
		})
	})
}
