package river

import (
	"context"
	"encoding/json"
	"testing"

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

func TestResumableSetStepTx(t *testing.T) {
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

	t.Run("SetsStep", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, make(map[string]any))

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		updatedJob, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, "step1")
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		reloadedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)

		var metadata map[string]any
		require.NoError(t, json.Unmarshal(reloadedJob.Metadata, &metadata))
		require.Equal(t, "step1", metadata[rivercommon.MetadataKeyResumableStep])
	})

	t.Run("SetsStepAndCursor", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, make(map[string]any))
		ctx = context.WithValue(ctx, resumableContextKey{}, &resumableState{cursors: make(map[string]json.RawMessage)})

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		type Cursor struct {
			ID int `json:"id"`
		}

		updatedJob, err := ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, "step2", Cursor{ID: 123})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		reloadedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)

		var metadata map[string]any
		require.NoError(t, json.Unmarshal(reloadedJob.Metadata, &metadata))
		require.Equal(t, "step2", metadata[rivercommon.MetadataKeyResumableStep])
		require.Equal(t, map[string]any{"step2": map[string]any{"id": float64(123)}}, metadata[rivercommon.MetadataKeyResumableCursor])

		metadataUpdates, ok := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
		require.True(t, ok)
		require.Equal(t, "step2", metadataUpdates[rivercommon.MetadataKeyResumableStep])
	})

	t.Run("ErrorIfNotRunning", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, "step1")
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
		_, err = ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, "step1")
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

		require.PanicsWithValue(t, "to use ResumableSetStepTx or ResumableSetStepCursorTx in a rivertest.Worker, the job must be inserted into the database first", func() {
			_, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, "step1")
			require.NoError(t, err)
		})
	})
}
