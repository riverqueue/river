package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

type cancelRunningJobArgs struct {
	testutil.JobArgsReflectKind[cancelRunningJobArgs]
}

func exerciseClientCancelRunningJob[TTx any](ctx context.Context, t *testing.T, driver riverdriver.Driver[TTx], schema string, pollOnly, transactional bool) {
	t.Helper()

	config := newTestConfig(t, schema)
	config.FetchPollInterval = time.Minute
	config.PollOnly = pollOnly
	config.Queues = map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}

	var jobStarted, jobContextCancelled testsignal.TestSignal[int64]
	jobStarted.Init(t)
	jobContextCancelled.Init(t)

	river.AddWorker(config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[cancelRunningJobArgs]) error {
		jobStarted.Signal(job.ID)
		<-ctx.Done()
		jobContextCancelled.Signal(job.ID)
		return ctx.Err()
	}))

	client, err := river.NewClient(driver, config)
	require.NoError(t, err)

	// An independent insert-only client cannot use the worker client's local
	// cancellation shortcut, exercising the same path as another process.
	controller, err := river.NewClient(driver, &river.Config{Schema: schema})
	require.NoError(t, err)

	// Insert before starting so initial fetching finds the job even with a long
	// fetch interval. Once running, the sole worker slot is occupied.
	insertRes, err := controller.Insert(ctx, &cancelRunningJobArgs{}, nil)
	require.NoError(t, err)

	events := subscribe(t, client)
	startClient(ctx, t, client)
	t.Cleanup(func() { require.NoError(t, client.StopAndCancel(ctx)) })
	require.Equal(t, insertRes.Job.ID, jobStarted.WaitOrTimeout())

	if transactional {
		// Rolling back must leave both the durable cancellation marker and the
		// worker untouched.
		execTx, err := driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = execTx.Rollback(ctx) })
		_, err = controller.JobCancelTx(ctx, driver.UnwrapTx(execTx), insertRes.Job.ID)
		require.NoError(t, err)
		require.NoError(t, execTx.Rollback(ctx))

		jobAfterRollback, err := controller.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, jobAfterRollback.State)
		require.NotContains(t, string(jobAfterRollback.Metadata), `"cancel_attempted_at"`)
		jobContextCancelled.RequireEmpty()

		execTx, err = driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = execTx.Rollback(ctx) })
		updatedJob, err := controller.JobCancelTx(ctx, driver.UnwrapTx(execTx), insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		// Keep the transaction open across a cancellation poll. Neither polling
		// nor notifications may cancel the worker before commit.
		select {
		case <-jobContextCancelled.WaitC():
			t.Fatal("worker cancelled before transaction committed")
		case <-time.After(2200 * time.Millisecond):
		}

		require.NoError(t, execTx.Commit(ctx))
	} else {
		updatedJob, err := controller.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)
	}

	require.Equal(t, insertRes.Job.ID, jobContextCancelled.WaitOrTimeout())
	event := riversharedtest.WaitOrTimeout(t, events)
	require.Equal(t, river.EventKindJobCancelled, event.Kind)
	require.Equal(t, insertRes.Job.ID, event.Job.ID)
	require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
}
