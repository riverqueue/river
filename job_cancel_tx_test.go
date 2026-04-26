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

func TestJobCancelTx(t *testing.T) {
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

	t.Run("CancelsJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		cancelErr := errors.New("cancelled by user")
		cancelledJob, err := JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, cancelErr)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)
		require.WithinDuration(t, time.Now(), *cancelledJob.FinalizedAt, 2*time.Second)
		require.Len(t, cancelledJob.Errors, 1)
		require.Equal(t, "JobCancelError: cancelled by user", cancelledJob.Errors[0].Error)
		require.Equal(t, job.Attempt, cancelledJob.Errors[0].Attempt)

		updatedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     job.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, updatedJob.State)
	})

	t.Run("CancelsJobWithNilError", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		cancelledJob, err := JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, nil)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)
		require.Len(t, cancelledJob.Errors, 1)
		require.Equal(t, "JobCancelError: <nil>", cancelledJob.Errors[0].Error)
	})

	t.Run("CancelsJobWithMetadataUpdates", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		metadataUpdates := map[string]any{"foo": "bar"}
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		cancelledJob, err := JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)

		updatedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     job.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, updatedJob.State)
		require.JSONEq(t, `{"foo":"bar"}`, string(updatedJob.Metadata))
	})

	t.Run("ErrorIfMetadataMarshallingFails", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, map[string]any{"foo": make(chan int)})

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		_, err := JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.ErrorContains(t, err, "unsupported type: chan int")
	})

	t.Run("ErrorIfNotRunning", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.EqualError(t, err, "job must be running")
	})

	t.Run("ErrorIfJobDoesntExist", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateAvailable),
		})

		_, err := bundle.exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID})
		require.NoError(t, err)

		job.State = rivertype.JobStateRunning
		_, err = JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("PanicsIfCalledInTestWorkerWithoutInsertingJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)
		ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		_, err := bundle.client.JobDeleteTx(ctx, bundle.tx, job.ID)
		require.NoError(t, err)
		job.State = rivertype.JobStateRunning

		require.PanicsWithValue(t, "to use JobCancelTx in a rivertest.Worker, the job must be inserted into the database first", func() {
			_, err := JobCancelTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, errors.New("oops"))
			require.NoError(t, err)
		})
	})
}
