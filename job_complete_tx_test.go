package river

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobCompleteTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		JobArgsReflectKind[JobArgs]
	}

	type testBundle struct {
		client *Client[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	setup := func(ctx context.Context, t *testing.T) (context.Context, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)
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

	t.Run("CompletesJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		completedJob, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.WithinDuration(t, time.Now(), *completedJob.FinalizedAt, 2*time.Second)

		updatedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     job.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, updatedJob.State)
	})

	t.Run("CompletesJobWithMetadataUpdates", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		// Inject valid metadata updates into context
		metadataUpdates := map[string]any{"foo": "bar"}
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		completedJob, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)

		updatedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     job.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, updatedJob.State)
	})

	t.Run("ErrorIfMetadataMarshallingFails", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		// Inject invalid metadata updates into context (using a channel which is not JSON marshalable)
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, map[string]any{"foo": make(chan int)})

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		_, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.ErrorContains(t, err, "unsupported type: chan int")
	})

	t.Run("ErrorIfNotRunning", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.EqualError(t, err, "job must be running")
	})

	t.Run("ErrorIfJobDoesntExist", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateAvailable),
		})

		// delete the job
		_, err := bundle.exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID})
		require.NoError(t, err)

		// fake the job's state to work around the check:
		job.State = rivertype.JobStateRunning
		_, err = JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("PanicsIfCalledInTestWorkerWithoutInsertingJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)
		ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		// delete the job as though it was never inserted:
		_, err := bundle.client.JobDeleteTx(ctx, bundle.tx, job.ID)
		require.NoError(t, err)
		job.State = rivertype.JobStateRunning

		require.PanicsWithValue(t, "to use JobCompleteTx in a rivertest.Worker, the job must be inserted into the database first", func() {
			_, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
			require.NoError(t, err)
		})
	})
}
