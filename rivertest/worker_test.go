package rivertest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

type testArgs struct {
	Value string `json:"value"`
}

func (testArgs) Kind() string { return "rivertest_work_test" }

func TestPanicError(t *testing.T) {
	t.Parallel()

	panicErr := &PanicError{Cause: errors.New("test panic error"), Trace: "test trace"}
	require.Equal(t, "rivertest.PanicError: test panic error\ntest trace", panicErr.Error())
}

func TestWorker_NewWorker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		config *river.Config
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		return &testBundle{
			config: &river.Config{ID: "rivertest-worker"},
			driver: riverpgxv5.New(nil),
			tx:     riverinternaltest.TestTx(ctx, t),
		}
	}

	t.Run("HandlesNilRiverConfig", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return nil
		})
		tw := NewWorker(t, bundle.driver, nil, worker)
		require.NotNil(t, tw.config)
	})
}

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
			require.Equal(t, 1, job.Attempt)
			require.NotNil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now(), *job.AttemptedAt, 5*time.Second)
			require.Equal(t, []string{"rivertest-worker"}, job.AttemptedBy)
			require.WithinDuration(t, time.Now(), job.CreatedAt, 5*time.Second)
			require.JSONEq(t, `{"value": "test"}`, string(job.EncodedArgs))
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Positive(t, job.ID)
			require.Equal(t, "rivertest_work_test", job.Kind)
			require.Equal(t, river.MaxAttemptsDefault, job.MaxAttempts)
			require.Equal(t, []byte(`{}`), job.Metadata)
			require.Equal(t, river.PriorityDefault, job.Priority)
			require.Equal(t, river.QueueDefault, job.Queue)
			require.WithinDuration(t, time.Now(), job.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			require.Equal(t, []string{}, job.Tags)
			require.Nil(t, job.UniqueKey)

			_, hasContextKeyInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool)
			require.True(t, hasContextKeyInsideTestWorker)

			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)
		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
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
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
		res, err = tw.Work(ctx, t, bundle.tx, testArgs{Value: "test2"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
	})

	t.Run("SetsCustomInsertOpts", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		hourFromNow := time.Now().UTC().Add(1 * time.Hour)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Equal(t, testArgs{Value: "test3"}, job.Args)
			require.Equal(t, 1, job.Attempt)
			require.NotNil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), *job.AttemptedAt, 2*time.Second)
			require.Equal(t, []string{"rivertest-worker"}, job.AttemptedBy)
			require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			require.JSONEq(t, `{"value": "test3"}`, string(job.EncodedArgs))
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Positive(t, job.ID)
			require.Equal(t, "rivertest_work_test", job.Kind)
			require.Equal(t, 420, job.MaxAttempts)
			require.JSONEq(t, `{"key": "value"}`, string(job.Metadata))
			require.Equal(t, 3, job.Priority)
			require.Equal(t, "custom_queue", job.Queue)
			require.WithinDuration(t, hourFromNow, job.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			require.Equal(t, []string{"tag1", "tag2"}, job.Tags)

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
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
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
			require.Empty(t, job.UniqueKey)
			require.Empty(t, job.UniqueStates)
			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, &river.InsertOpts{
			UniqueOpts: river.UniqueOpts{ByPeriod: 1 * time.Hour},
		})
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
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
		require.Equal(t, river.EventKindJobSnoozed, res.EventKind)
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
		require.Equal(t, river.EventKindJobCancelled, res.EventKind)
	})

	t.Run("UsesACustomClockWhenProvided", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		hourFromNow := time.Now().UTC().Add(1 * time.Hour)
		timeStub := &TimeStub{}
		timeStub.StubNowUTC(hourFromNow)
		bundle.config.Test.Time = timeStub

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.WithinDuration(t, hourFromNow, *job.AttemptedAt, time.Millisecond)
			require.WithinDuration(t, hourFromNow, job.CreatedAt, time.Millisecond)
			require.WithinDuration(t, hourFromNow, job.ScheduledAt, time.Millisecond)
			return nil
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
		require.WithinDuration(t, hourFromNow, *res.Job.FinalizedAt, time.Millisecond)
	})

	t.Run("ErrorFromWorker", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		errToReturn := errors.New("test error")
		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return errToReturn
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.ErrorIs(t, err, errToReturn)
		require.Equal(t, river.EventKindJobFailed, res.EventKind)
	})

	t.Run("PanicFromWorker", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		errToReturn := errors.New("test panic error")
		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			panic(errToReturn)
		})
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.ErrorIs(t, err, &PanicError{})
		require.ErrorContains(t, err, "test panic error")
		require.Equal(t, river.EventKindJobFailed, res.EventKind)

		var panicErr *PanicError
		require.ErrorAs(t, err, &panicErr)
		require.Equal(t, errToReturn, panicErr.Cause)
		require.Contains(t, panicErr.Trace, "github.com/riverqueue/river/rivertest.TestWorker_Work")
		require.Len(t, res.Job.Errors, 1)
		require.Contains(t, res.Job.Errors[0].Error, "test panic error")
	})

	t.Run("ErrorsWithAlreadyClosedTransaction", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Immediately roll back the transaction to force an error:
		require.NoError(t, bundle.tx.Rollback(ctx))

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error { return nil })
		tw := NewWorker(t, bundle.driver, bundle.config, worker)

		res, err := tw.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.ErrorContains(t, err, "failed to insert job: tx is closed")
		require.Nil(t, res)
	})

	t.Run("WorkUsesHooks", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		type HookWithBaseService struct {
			baseservice.BaseService
			river.HookWorkBeginFunc
		}
		var (
			hookCalled                bool
			hookWithBaseService       = &HookWithBaseService{}
			hookWithBaseServiceCalled bool
		)
		hookWithBaseService.HookWorkBeginFunc = func(ctx context.Context, job *rivertype.JobRow) error {
			hookWithBaseServiceCalled = true
			require.NotEmpty(t, hookWithBaseService.Name) // if name is non-empty, it means the base service was initialized properly
			return nil
		}

		bundle.config.Hooks = []rivertype.Hook{
			hookWithBaseService,
			river.HookWorkBeginFunc(func(ctx context.Context, job *rivertype.JobRow) error {
				hookCalled = true
				return nil
			}),
		}

		var (
			worker = river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
				return nil
			})
			testWorker = NewWorker(t, bundle.driver, bundle.config, worker)
		)

		_, err := testWorker.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.True(t, hookCalled)
		require.True(t, hookWithBaseServiceCalled)
	})

	t.Run("WorkUsesMiddleware", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		type MiddlewareWithBaseService struct {
			baseservice.BaseService
			river.WorkerMiddlewareFunc
		}
		var (
			middlewareCalled                bool
			middlewareWithBaseService       = &MiddlewareWithBaseService{}
			middlewareWithBaseServiceCalled bool
		)
		middlewareWithBaseService.WorkerMiddlewareFunc = func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
			middlewareWithBaseServiceCalled = true
			require.NotEmpty(t, middlewareWithBaseService.Name) // if name is non-empty, it means the base service was initialized properly
			return doInner(ctx)
		}

		bundle.config.Middleware = []rivertype.Middleware{
			middlewareWithBaseService,
			river.WorkerMiddlewareFunc(func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
				middlewareCalled = true
				return doInner(ctx)
			}),
		}

		var (
			worker = river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
				return nil
			})
			testWorker = NewWorker(t, bundle.driver, bundle.config, worker)
		)

		_, err := testWorker.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.True(t, middlewareCalled)
		require.True(t, middlewareWithBaseServiceCalled)
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
			require.WithinDuration(t, time.Now(), *job.AttemptedAt, 5*time.Second)
			require.Equal(t, []string{"rivertest-workjob"}, job.AttemptedBy)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			return nil
		}

		insertRes, err := bundle.client.InsertTx(ctx, bundle.tx, testArgs{}, nil)
		require.NoError(t, err)

		res, err := testWorker.WorkJob(ctx, t, bundle.tx, insertRes.Job)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)
	})

	t.Run("JobCompleteTxWithInsertedJobRow", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t)

		args := testArgs{}
		insertRes, err := bundle.client.InsertTx(ctx, bundle.tx, args, nil)
		require.NoError(t, err)

		bundle.workFunc = func(ctx context.Context, job *river.Job[testArgs]) error {
			updatedJob, err := bundle.driver.UnwrapExecutor(bundle.tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: insertRes.Job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

			_, err = river.JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, job)
			require.NoError(t, err)

			return nil
		}

		res, err := testWorker.WorkJob(ctx, t, bundle.tx, insertRes.Job)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, res.EventKind)

		updatedJob, err := bundle.driver.UnwrapExecutor(bundle.tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: insertRes.Job.ID, Schema: ""})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, updatedJob.State)
	})

	t.Run("ErrorsWhenGivenAlreadyCompletedJob", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		testWorker, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.driver.UnwrapExecutor(bundle.tx), &testfactory.JobOpts{
			EncodedArgs: []byte(`{"value": "test"}`),
			Kind:        ptrutil.Ptr("rivertest_work_test"),
			State:       ptrutil.Ptr(rivertype.JobStateCompleted),
		})

		res, err := testWorker.WorkJob(ctx, t, bundle.tx, job)
		require.ErrorContains(t, err, "failed to update job to running state")
		require.Nil(t, res)
	})
}
