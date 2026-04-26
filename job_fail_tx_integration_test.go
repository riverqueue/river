package river

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest/retrypolicytest"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

// TestJobFailTxIntegration tests edge cases: every combination of:
// (Work returns nil / err / panic / JobCancelError) *
// (Tx fn called: none / JobFailTx / JobCancelTx / JobCompleteTx) *
// (tx commit or rollback), asserting the job's final DB state.
func TestJobFailTxIntegration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool        *pgxpool.Pool
		subscribeChan <-chan *Event
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)
		config.Schema = schema
		// Use a predictable retry policy so a retryable job isn't moved back
		// to available by the scheduler before assertions run.
		config.RetryPolicy = &retrypolicytest.RetryPolicySlow{}

		client := newTestClient(t, dbPool, config)
		startClient(ctx, t, client)

		subscribeChan, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		return client, &testBundle{
			dbPool:        dbPool,
			subscribeChan: subscribeChan,
		}
	}

	// Work returns nil, no Tx fn called.
	t.Run("NilReturnNoTxCallCompletes", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		client, bundle := setup(t, config)

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)
	})

	// Work returns nil, JobFailTx committed (non-final attempt).
	t.Run("NilReturnFailTxCommittedNonFinal", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("boom"))
			require.NoError(t, err)

			return tx.Commit(ctx)
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)

		reloaded, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, reloaded.State)
		require.Len(t, reloaded.Errors, 1)
		require.Equal(t, "boom", reloaded.Errors[0].Error)
	})

	// Final attempt -> Discarded.
	t.Run("NilReturnFailTxCommittedFinalAttempt", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("final boom"))
			require.NoError(t, err)

			return tx.Commit(ctx)
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 1})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateDiscarded, event.Job.State)
		require.NotNil(t, event.Job.FinalizedAt)
	})

	// Work returns nil, JobFailTx called but tx rolled back.
	//nolint:dupl // matrix test; near-identical structure is the point
	t.Run("NilReturnFailTxRolledBack", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("will be rolled back"))
			require.NoError(t, err)

			return tx.Rollback(ctx)
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 1})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)
	})

	// Work returns nil, JobCancelTx committed.
	t.Run("NilReturnCancelTxCommitted", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobCancelTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("abort"))
			require.NoError(t, err)

			return tx.Commit(ctx)
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
	})

	// Work returns nil, JobCancelTx called but tx rolled back.
	//nolint:dupl // matrix test; near-identical structure is the point
	t.Run("NilReturnCancelTxRolledBack", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)

			_, err = JobCancelTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("will be rolled back"))
			require.NoError(t, err)

			return tx.Rollback(ctx)
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)
	})

	// Work returns err, JobFailTx committed.
	// JobFailTx's state wins; the executor's subsequent state/errors
	// writes are guarded by state = 'running' and don't take effect.
	t.Run("ErrReturnFailTxCommitted", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("from JobFailTx"))
			require.NoError(t, err)

			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return errors.New("post-tx err")
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)

		reloaded, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, reloaded.State)
		require.Len(t, reloaded.Errors, 1)
		// The error from JobFailTx wins; the subsequent work-returned
		// error is not appended because the executor's errors-update is
		// guarded by state = 'running'.
		require.Equal(t, "from JobFailTx", reloaded.Errors[0].Error)
	})

	// Work returns err, JobFailTx called but tx rolled back.
	// Executor's error path records the work error as usual.
	//nolint:dupl // matrix test; near-identical structure is the point
	t.Run("ErrReturnFailTxRolledBack", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("ignored by rollback"))
			require.NoError(t, err)

			if err := tx.Rollback(ctx); err != nil {
				return err
			}
			return errors.New("from worker")
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)
		require.Len(t, event.Job.Errors, 1)
		require.Equal(t, "from worker", event.Job.Errors[0].Error)
	})

	// Work returns err, JobCancelTx committed.
	t.Run("ErrReturnCancelTxCommitted", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobCancelTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("from JobCancelTx"))
			require.NoError(t, err)

			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return errors.New("post-tx err")
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
	})

	// Work returns err, JobCancelTx called but tx rolled back.
	//nolint:dupl // matrix test; near-identical structure is the point
	t.Run("ErrReturnCancelTxRolledBack", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)

			_, err = JobCancelTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("ignored by rollback"))
			require.NoError(t, err)

			if err := tx.Rollback(ctx); err != nil {
				return err
			}
			return errors.New("from worker")
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)
		require.Len(t, event.Job.Errors, 1)
		require.Equal(t, "from worker", event.Job.Errors[0].Error)
	})

	// Work panics after JobFailTx committed.
	t.Run("PanicAfterFailTxCommitted", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("from JobFailTx (pre-panic)"))
			require.NoError(t, err)

			if err := tx.Commit(ctx); err != nil {
				return err
			}
			panic("boom")
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)

		reloaded, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, reloaded.State)
		require.Len(t, reloaded.Errors, 1)
		require.Equal(t, "from JobFailTx (pre-panic)", reloaded.Errors[0].Error)
	})

	// Work returns JobCancelError after calling JobFailTx.
	// DB state ends at whatever JobFailTx set (retryable); the
	// executor's cancel branch emits JobSetStateCancelled but the
	// state change is guarded by state = 'running' and doesn't apply.
	t.Run("JobCancelErrorReturnedAfterFailTxCommitted", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var bundle *testBundle
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("from JobFailTx"))
			require.NoError(t, err)

			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return JobCancel(errors.New("ignored cancel"))
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)
	})

	// JobFailTx then JobCompleteTx in the same tx. The second call
	// sees the row in a non-running state: pilot.JobSetStateIfRunningMany's
	// SQL returns the row unchanged (IfRunning check fails in the SQL), so
	// JobCompleteTx returns a job whose State is *not* Completed, without
	// error. The DB state after commit reflects what JobFailTx set.
	t.Run("FailTxThenCompleteTxSameTx", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var (
			bundle             *testBundle
			secondCallJobState rivertype.JobState
			secondCallErr      error
			secondCallDone     = make(chan struct{})
		)
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			defer close(secondCallDone)

			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			_, err = JobFailTx[*riverpgxv5.Driver](ctx, tx, job, errors.New("first"))
			require.NoError(t, err)

			var secondJob *Job[JobArgs]
			secondJob, secondCallErr = JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
			if secondJob != nil {
				secondCallJobState = secondJob.State
			}

			return tx.Commit(ctx)
		}))

		client, b := setup(t, config)
		bundle = b

		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{MaxAttempts: 3})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)

		<-secondCallDone
		require.NoError(t, secondCallErr)
		require.Equal(t, rivertype.JobStateRetryable, secondCallJobState)

		reloaded, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, reloaded.State)
	})
}
