package rivertest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type testArgs struct {
	Value string `json:"value"`
}

func (testArgs) Kind() string { return "rivertest_work_test" }

func TestWorker_Work(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		config *river.Config
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			config = &river.Config{ID: "rivertest-worker"}
			driver = riverpgxv5.New(nil)
			tx     = riverinternaltest.TestTx(ctx, t)
		)

		return &testBundle{
			config: config,
			driver: driver,
			tx:     tx,
		}
	}

	t.Run("WorkASimpleJob", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Equal(t, testArgs{Value: "test"}, job.Args)
			require.Equal(t, 1, job.JobRow.Attempt)
			require.NotNil(t, job.JobRow.AttemptedAt)
			require.WithinDuration(t, time.Now(), *job.JobRow.AttemptedAt, 5*time.Second)
			require.Equal(t, []string{"rivertest-worker"}, job.JobRow.AttemptedBy)
			require.WithinDuration(t, time.Now(), job.JobRow.CreatedAt, 5*time.Second)
			require.JSONEq(t, `{"value": "test"}`, string(job.JobRow.EncodedArgs))
			require.Empty(t, job.JobRow.Errors)
			require.Nil(t, job.JobRow.FinalizedAt)
			require.Positive(t, job.JobRow.ID)
			require.Equal(t, "rivertest_work_test", job.JobRow.Kind)
			require.Equal(t, river.MaxAttemptsDefault, job.JobRow.MaxAttempts)
			require.Equal(t, []byte(`{}`), job.JobRow.Metadata)
			require.Equal(t, river.PriorityDefault, job.JobRow.Priority)
			require.Equal(t, river.QueueDefault, job.JobRow.Queue)
			require.WithinDuration(t, time.Now(), job.JobRow.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateRunning, job.JobRow.State)
			require.Equal(t, []string{}, job.JobRow.Tags)
			require.Nil(t, job.JobRow.UniqueKey)

			_, hasContextKeyInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool)
			require.True(t, hasContextKeyInsideTestWorker)

			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)
		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
	})

	t.Run("Reusable", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)
		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
		res, err = tw.Work(ctx, t, bundle.tx, testArgs{Value: "test2"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
	})

	t.Run("SetsCustomInsertOpts", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		hourFromNow := time.Now().UTC().Add(1 * time.Hour)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Equal(t, testArgs{Value: "test3"}, job.Args)
			require.Equal(t, 1, job.JobRow.Attempt)
			require.NotNil(t, job.JobRow.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), *job.JobRow.AttemptedAt, 2*time.Second)
			require.Equal(t, []string{"rivertest-worker"}, job.JobRow.AttemptedBy)
			require.WithinDuration(t, time.Now().UTC(), job.JobRow.CreatedAt, 2*time.Second)
			require.JSONEq(t, `{"value": "test3"}`, string(job.JobRow.EncodedArgs))
			require.Empty(t, job.JobRow.Errors)
			require.Nil(t, job.JobRow.FinalizedAt)
			require.Positive(t, job.JobRow.ID)
			require.Equal(t, "rivertest_work_test", job.JobRow.Kind)
			require.Equal(t, 420, job.JobRow.MaxAttempts)
			require.JSONEq(t, `{"key": "value"}`, string(job.JobRow.Metadata))
			require.Equal(t, 3, job.JobRow.Priority)
			require.Equal(t, "custom_queue", job.JobRow.Queue)
			require.WithinDuration(t, hourFromNow, job.JobRow.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateRunning, job.JobRow.State)
			require.Equal(t, []string{"tag1", "tag2"}, job.JobRow.Tags)

			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		// You can also pass in custom insert options:
		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test3"}, &river.InsertOpts{
			MaxAttempts: 420,
			Metadata:    []byte(`{"key": "value"}`),
			Pending:     true, // ignored but added to ensure non-default behavior
			Priority:    3,
			Queue:       "custom_queue",
			ScheduledAt: hourFromNow,
			Tags:        []string{"tag1", "tag2"},
		})
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
	})

	t.Run("UniqueOptsAreIgnored", func(t *testing.T) {
		t.Parallel()
		// UniqueOpts must be ignored because otherwise there's a likelihood of
		// conflicts with parallel tests inserting jobs with the same unique key.

		bundle := setup(t)

		stubTime := &riversharedtest.TimeStub{}
		now := time.Now().UTC()
		stubTime.StubNowUTC(now)
		bundle.config.Test.Time = stubTime

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Empty(t, job.JobRow.UniqueKey)
			require.Empty(t, job.JobRow.UniqueStates)
			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, &river.InsertOpts{
			UniqueOpts: river.UniqueOpts{ByPeriod: 1 * time.Hour},
		})
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
	})

	t.Run("ReturnsASnoozeEventKindWhenSnoozed", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return river.JobSnooze(time.Hour)
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobSnoozed, res.Kind)
	})

	t.Run("ReturnsACancelEventKindWhenCancelled", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return river.JobCancel(nil)
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCancelled, res.Kind)
	})

	t.Run("UsesACustomClockWhenProvided", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		hourFromNow := time.Now().UTC().Add(1 * time.Hour)
		timeStub := &TimeStub{}
		timeStub.StubNowUTC(hourFromNow)
		bundle.config.Test.Time = timeStub

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.WithinDuration(t, hourFromNow, *job.JobRow.AttemptedAt, time.Millisecond)
			require.WithinDuration(t, hourFromNow, job.JobRow.CreatedAt, time.Millisecond)
			require.WithinDuration(t, hourFromNow, job.JobRow.ScheduledAt, time.Millisecond)
			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
		require.WithinDuration(t, hourFromNow, *res.Job.FinalizedAt, time.Millisecond)
	})
}

func TestWorker_WorkJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		client   *river.Client[pgx.Tx]
		config   *river.Config
		driver   *riverpgxv5.Driver
		tx       pgx.Tx
		workFunc func(ctx context.Context, job *river.Job[testArgs]) error
	}

	setup := func(t *testing.T) (*Worker[testArgs, pgx.Tx], *testBundle) {
		t.Helper()

		var (
			config = &river.Config{ID: "rivertest-workjob"}
			driver = riverpgxv5.New(nil)
		)

		client, err := river.NewClient(driver, config)
		require.NoError(t, err)

		bundle := &testBundle{
			client:   client,
			config:   config,
			driver:   driver,
			tx:       riverinternaltest.TestTx(ctx, t),
			workFunc: func(ctx context.Context, job *river.Job[testArgs]) error { return nil },
		}

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return bundle.workFunc(ctx, job)
		})

		return NewWorker(t, driver, config, worker), bundle
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t)

		bundle.workFunc = func(ctx context.Context, job *river.Job[testArgs]) error {
			require.WithinDuration(t, time.Now(), *job.JobRow.AttemptedAt, 5*time.Second)
			require.Equal(t, []string{"rivertest-workjob"}, job.JobRow.AttemptedBy)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			return nil
		}

		insertRes, err := bundle.client.InsertTx(ctx, bundle.tx, testArgs{}, nil)
		require.NoError(t, err)

		res, err := testWorker.WorkJob(ctx, t, bundle.tx, insertRes.Job)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)
	})

	t.Run("JobCompleteTxWithInsertedJobRow", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t)

		args := testArgs{}
		insertRes, err := bundle.client.InsertTx(ctx, bundle.tx, args, nil)
		require.NoError(t, err)

		bundle.workFunc = func(ctx context.Context, job *river.Job[testArgs]) error {
			updatedJob, err := bundle.driver.UnwrapExecutor(bundle.tx).JobGetByID(ctx, insertRes.Job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

			_, err = river.JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, job)
			require.NoError(t, err)

			return nil
		}

		res, err := testWorker.WorkJob(ctx, t, bundle.tx, insertRes.Job)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.Kind)

		updatedJob, err := bundle.driver.UnwrapExecutor(bundle.tx).JobGetByID(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, updatedJob.State)
	})
}
