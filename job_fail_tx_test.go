package river

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobFailTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	type testBundle struct {
		client *Client[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	// retryFunc injects a NextRetryFunc into the context so JobFailTx can use
	// it like the executor would. Tests can override scheduledAt/useAvailable
	// to exercise the Retryable vs Available branch.
	injectRetry := func(ctx context.Context, scheduledAt time.Time, useAvailable bool) context.Context {
		fn := jobexecutor.NextRetryFunc(func(ctx context.Context, now time.Time, jobRow *rivertype.JobRow) (time.Time, bool) {
			return scheduledAt, useAvailable
		})
		return context.WithValue(ctx, jobexecutor.ContextKeyNextRetry, fn)
	}

	setup := func(ctx context.Context, t *testing.T) (context.Context, *testBundle) {
		t.Helper()

		tx := riverdbtest.TestTxPgx(ctx, t)
		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
		})
		require.NoError(t, err)
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)

		return ctx, &testBundle{
			client: client,
			exec:   riverpgxv5.New(nil).UnwrapExecutor(tx),
			tx:     tx,
		}
	}

	t.Run("RetryableOnNonFinalAttempt", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		nextRetry := time.Now().Add(time.Hour)
		ctx = injectRetry(ctx, nextRetry, false)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(1),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		failErr := errors.New("temporary failure")
		failedJob, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, failErr)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, failedJob.State)
		require.WithinDuration(t, nextRetry, failedJob.ScheduledAt, time.Second)
		require.Nil(t, failedJob.FinalizedAt)
		require.Len(t, failedJob.Errors, 1)
		require.Equal(t, "temporary failure", failedJob.Errors[0].Error)
		require.Equal(t, job.Attempt, failedJob.Errors[0].Attempt)
	})

	t.Run("AvailableOnNonFinalAttemptShortRetry", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		nextRetry := time.Now().Add(50 * time.Millisecond)
		ctx = injectRetry(ctx, nextRetry, true)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(1),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		failedJob, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("short retry"))
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, failedJob.State)
		require.Nil(t, failedJob.FinalizedAt)
	})

	t.Run("DiscardsJobOnFinalAttempt", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		// Final attempt: attempt == max_attempts. No retry fn lookup should
		// be needed on the discard path.
		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		failErr := errors.New("permanent failure")
		failedJob, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, failErr)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, failedJob.State)
		require.WithinDuration(t, time.Now(), *failedJob.FinalizedAt, 2*time.Second)
		require.Len(t, failedJob.Errors, 1)
		require.Equal(t, "permanent failure", failedJob.Errors[0].Error)

		reloaded, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, reloaded.State)
	})

	t.Run("FailsJobWithNilError", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		failedJob, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, nil)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, failedJob.State)
		require.Len(t, failedJob.Errors, 1)
		require.Equal(t, "<nil>", failedJob.Errors[0].Error)
	})

	t.Run("FailsJobWithMetadataUpdates", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		metadataUpdates := map[string]any{"foo": "bar"}
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		failedJob, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, failedJob.State)

		reloaded, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"bar"}`, string(reloaded.Metadata))
	})

	t.Run("ErrorIfMetadataMarshallingFails", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, map[string]any{"foo": make(chan int)})

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		_, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.ErrorContains(t, err, "unsupported type: chan int")
	})

	t.Run("ErrorIfNotRunning", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.EqualError(t, err, "job must be running")
	})

	t.Run("ErrorIfNoNextRetryFuncOnNonFinalAttempt", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(1),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
		})

		_, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.EqualError(t, err, "next retry function not found in context, can only work within a River worker")
	})

	t.Run("ErrorIfJobDoesntExist", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateAvailable),
		})

		_, err := bundle.exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID})
		require.NoError(t, err)

		job.State = rivertype.JobStateRunning
		_, err = JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("PanicsIfCalledInTestWorkerWithoutInsertingJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)
		ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			State:       ptrutil.Ptr(rivertype.JobStateAvailable),
		})
		_, err := bundle.client.JobDeleteTx(ctx, bundle.tx, job.ID)
		require.NoError(t, err)
		job.State = rivertype.JobStateRunning

		require.PanicsWithValue(t, "to use JobFailTx in a rivertest.Worker, the job must be inserted into the database first", func() {
			_, err := JobFailTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
			require.NoError(t, err)
		})
	})
}
