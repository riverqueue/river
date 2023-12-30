package river

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

type customRetryPolicyWorker struct {
	WorkerDefaults[callbackArgs]
	f         func() error
	nextRetry func() time.Time
}

func (w *customRetryPolicyWorker) NextRetry(job *Job[callbackArgs]) time.Time {
	if w.nextRetry != nil {
		return w.nextRetry()
	}
	return time.Time{}
}

func (w *customRetryPolicyWorker) Work(ctx context.Context, job *Job[callbackArgs]) error {
	return w.f()
}

// Makes a workerInfo using the real workerWrapper with a job that uses a
// callback Work func and allows for customizable maxAttempts and nextRetry.
func newWorkUnitFactoryWithCustomRetry(f func() error, nextRetry func() time.Time) workunit.WorkUnitFactory {
	return &workUnitFactoryWrapper[callbackArgs]{worker: &customRetryPolicyWorker{
		f:         f,
		nextRetry: nextRetry,
	}}
}

// A retry policy demonstrating trivial customization.
type retryPolicyCustom struct {
	DefaultClientRetryPolicy
}

func (p *retryPolicyCustom) NextRetry(job *rivertype.JobRow) time.Time {
	var backoffDuration time.Duration
	switch job.Attempt {
	case 1:
		backoffDuration = 10 * time.Second
	case 2:
		backoffDuration = 20 * time.Second
	case 3:
		backoffDuration = 30 * time.Second
	default:
		panic(fmt.Sprintf("next retry should not have been called for attempt %d", job.Attempt))
	}

	return job.AttemptedAt.Add(backoffDuration)
}

// A retry policy that returns invalid timestamps.
type retryPolicyInvalid struct {
	DefaultClientRetryPolicy
}

func (p *retryPolicyInvalid) NextRetry(job *rivertype.JobRow) time.Time { return time.Time{} }

// Identical to default retry policy except that it leaves off the jitter to
// make checking against it more convenient.
type retryPolicyNoJitter struct {
	DefaultClientRetryPolicy
}

func (p *retryPolicyNoJitter) NextRetry(job *rivertype.JobRow) time.Time {
	return job.AttemptedAt.Add(timeutil.SecondsAsDuration(p.retrySecondsWithoutJitter(job.Attempt)))
}

type testErrorHandler struct {
	HandleErrorCalled bool
	HandleErrorFunc   func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult

	HandlePanicCalled bool
	HandlePanicFunc   func(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult
}

// Test handler with no-ops for both error handling functions.
func newTestErrorHandler() *testErrorHandler {
	return &testErrorHandler{
		HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult { return nil },
		HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult { return nil },
	}
}

func (h *testErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
	h.HandleErrorCalled = true
	return h.HandleErrorFunc(ctx, job, err)
}

func (h *testErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult {
	h.HandlePanicCalled = true
	return h.HandlePanicFunc(ctx, job, panicVal)
}

func TestJobExecutor_Execute(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type testBundle struct {
		adapter           *dbadapter.StandardAdapter
		completer         *jobcompleter.InlineJobCompleter
		errorHandler      *testErrorHandler
		getUpdatesAndStop func() []jobcompleter.CompleterJobUpdated
		jobRow            *rivertype.JobRow
		tx                pgx.Tx
	}

	setup := func(t *testing.T) (*jobExecutor, *testBundle) {
		t.Helper()

		var (
			tx        = riverinternaltest.TestTx(ctx, t)
			archetype = riverinternaltest.BaseServiceArchetype(t)
			adapter   = dbadapter.NewStandardAdapter(archetype, &dbadapter.StandardAdapterConfig{Executor: tx})
			completer = jobcompleter.NewInlineCompleter(archetype, adapter)
		)

		var updates []jobcompleter.CompleterJobUpdated
		completer.Subscribe(func(update jobcompleter.CompleterJobUpdated) {
			updates = append(updates, update)
		})

		getJobUpdates := func() []jobcompleter.CompleterJobUpdated {
			completer.Wait()
			return updates
		}
		t.Cleanup(func() { _ = getJobUpdates() })

		workUnitFactory := newWorkUnitFactoryWithCustomRetry(func() error { return nil }, nil)

		job, err := queries.JobInsert(ctx, tx, dbsqlc.JobInsertParams{
			Args:        []byte("{}"),
			Attempt:     0,
			AttemptedAt: ptrutil.Ptr(archetype.TimeNowUTC()),
			Kind:        (callbackArgs{}).Kind(),
			MaxAttempts: int16(rivercommon.MaxAttemptsDefault),
			Priority:    int16(rivercommon.PriorityDefault),
			Queue:       rivercommon.QueueDefault,
			State:       dbsqlc.JobStateAvailable,
		})
		require.NoError(t, err)

		// Fetch the job to make sure it's marked as running:
		jobs, err := queries.JobGetAvailable(ctx, tx, dbsqlc.JobGetAvailableParams{
			LimitCount: 1,
			Queue:      rivercommon.QueueDefault,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, job.ID, jobs[0].ID)
		job = jobs[0]

		bundle := &testBundle{
			adapter:           adapter,
			completer:         completer,
			errorHandler:      newTestErrorHandler(),
			getUpdatesAndStop: getJobUpdates,
			jobRow:            dbsqlc.JobRowFromInternal(job),
			tx:                tx,
		}

		executor := baseservice.Init(archetype, &jobExecutor{
			Adapter:                bundle.adapter,
			ClientRetryPolicy:      &retryPolicyNoJitter{},
			Completer:              bundle.completer,
			ErrorHandler:           bundle.errorHandler,
			InformProducerDoneFunc: func(job *rivertype.JobRow) {},
			JobRow:                 bundle.jobRow,
			SchedulerInterval:      riverinternaltest.SchedulerShortInterval,
			WorkUnit:               workUnitFactory.MakeUnit(bundle.jobRow),
		})

		return executor, bundle
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		// A simple `return nil` occasionally clocks in at exactly 0s of run
		// duration and fails the assertion on non-zero below. To avoid that,
		// make sure we sleep a tiny amount of time.
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			time.Sleep(1 * time.Microsecond)
			return nil
		}, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateCompleted, job.State)

		jobUpdates := bundle.getUpdatesAndStop()
		require.Len(t, jobUpdates, 1)
		jobUpdate := jobUpdates[0]
		t.Logf("Job statistics: %+v", jobUpdate.JobStats)
		require.NotZero(t, jobUpdate.JobStats.CompleteDuration)
		require.NotZero(t, jobUpdate.JobStats.QueueWaitDuration)
		require.NotZero(t, jobUpdate.JobStats.RunDuration)
	})

	t.Run("FirstError", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		baselineTime := riverinternaltest.StubTime(&executor.Archetype, time.Now())

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
		require.Len(t, job.Errors, 1)
		require.Equal(t, baselineTime, job.Errors[0].At)
		require.Equal(t, uint16(1), job.Errors[0].Attempt)
		require.Equal(t, "job error", job.Errors[0].Error)
		require.Equal(t, "", job.Errors[0].Trace)
	})

	t.Run("ErrorAgainAfterRetry", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = 2

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
	})

	t.Run("ErrorSetsJobAvailableBelowSchedulerIntervalThreshold", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.SchedulerInterval = 3 * time.Second

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		{
			executor.Execute(ctx)
			executor.Completer.Wait()

			job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
			require.NoError(t, err)
			require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
			require.Equal(t, dbsqlc.JobStateAvailable, job.State)
		}

		_, err := queries.JobSetState(ctx, bundle.tx, dbsqlc.JobSetStateParams{
			ID:    bundle.jobRow.ID,
			State: dbsqlc.JobStateRunning,
		})
		require.NoError(t, err)

		bundle.jobRow.Attempt = 2

		{
			executor.Execute(ctx)
			executor.Completer.Wait()

			job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
			require.NoError(t, err)
			require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 16*time.Second)
			require.Equal(t, dbsqlc.JobStateRetryable, job.State)
		}
	})

	t.Run("ErrorDiscardsJobAfterTooManyAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = bundle.jobRow.MaxAttempts

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateDiscarded, job.State)
	})

	t.Run("JobCancelErrorCancelsJobEvenWithRemainingAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		// ensure we still have remaining attempts:
		require.Greater(t, bundle.jobRow.MaxAttempts, bundle.jobRow.Attempt)

		cancelErr := JobCancel(errors.New("throw away this job"))
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, dbsqlc.JobStateCancelled, job.State)
		require.Len(t, job.Errors, 1)
		require.WithinDuration(t, time.Now(), job.Errors[0].At, 2*time.Second)
		require.Equal(t, uint16(1), job.Errors[0].Attempt)
		require.Equal(t, "jobCancelError: throw away this job", job.Errors[0].Error)
		require.Equal(t, "", job.Errors[0].Trace)
	})

	t.Run("JobSnoozeErrorReschedulesJobAndIncrementsMaxAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		maxAttemptsBefore := bundle.jobRow.MaxAttempts

		cancelErr := JobSnooze(30 * time.Minute)
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateScheduled, job.State)
		require.WithinDuration(t, time.Now().Add(30*time.Minute), job.ScheduledAt, 2*time.Second)
		require.Equal(t, maxAttemptsBefore+1, int(job.MaxAttempts))
		require.Empty(t, job.Errors)
	})

	t.Run("ErrorWithCustomRetryPolicy", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.ClientRetryPolicy = &retryPolicyCustom{}

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
	})

	t.Run("ErrorWithCustomNextRetryReturnedFromWorker", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		workerErr := errors.New("job error")
		nextRetryAt := time.Now().Add(1 * time.Hour).UTC()
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, func() time.Time {
			return nextRetryAt
		}).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
		require.WithinDuration(t, nextRetryAt, job.ScheduledAt, time.Microsecond)
	})

	t.Run("InvalidNextRetryAt", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.ClientRetryPolicy = &retryPolicyInvalid{}

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, (&DefaultClientRetryPolicy{}).NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
	})

	t.Run("ErrorWithErrorHandler", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandleErrorFunc = func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
			require.Equal(t, workerErr, err)
			return nil
		}

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandleErrorCalled)
	})

	t.Run("ErrorWithErrorHandlerSetCancelled", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandleErrorFunc = func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
			return &ErrorHandlerResult{SetCancelled: true}
		}

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateCancelled, job.State)

		require.True(t, bundle.errorHandler.HandleErrorCalled)
	})

	t.Run("ErrorWithErrorHandlerPanic", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandleErrorFunc = func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
			panic("error handled panicked!")
		}

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandleErrorCalled)
	})

	t.Run("Panic", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
		require.Len(t, job.Errors, 1)
		// Sufficient enough to ensure that the stack trace is included:
		require.Contains(t, job.Errors[0].Trace, "river/job_executor.go")
	})

	t.Run("PanicAgainAfterRetry", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = 2

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)
	})

	t.Run("PanicDiscardsJobAfterTooManyAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = bundle.jobRow.MaxAttempts

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 1*time.Second)
		require.Equal(t, dbsqlc.JobStateDiscarded, job.State)
	})

	t.Run("PanicWithPanicHandler", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandlePanicFunc = func(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult {
			require.Equal(t, "panic val", panicVal)
			return nil
		}

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandlePanicCalled)
	})

	t.Run("PanicWithPanicHandlerSetCancelled", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandlePanicFunc = func(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult {
			return &ErrorHandlerResult{SetCancelled: true}
		}

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateCancelled, job.State)

		require.True(t, bundle.errorHandler.HandlePanicCalled)
	})

	t.Run("PanicWithPanicHandlerPanic", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandlePanicFunc = func(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult {
			panic("panic handler panicked!")
		}

		executor.Execute(ctx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandlePanicCalled)
	})

	runCancelTest := func(t *testing.T, returnErr error) *dbsqlc.RiverJob { //nolint:thelper
		executor, bundle := setup(t)

		// ensure we still have remaining attempts:
		require.Greater(t, bundle.jobRow.MaxAttempts, bundle.jobRow.Attempt)

		jobStarted := make(chan struct{})
		haveCancelled := make(chan struct{})
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			close(jobStarted)
			<-haveCancelled
			return returnErr
		}, nil).MakeUnit(bundle.jobRow)

		go func() {
			<-jobStarted
			executor.Cancel()
			close(haveCancelled)
		}()

		workCtx, cancelFunc := context.WithCancelCause(ctx)
		executor.CancelFunc = cancelFunc

		executor.Execute(workCtx)
		executor.Completer.Wait()

		job, err := queries.JobGetByID(ctx, bundle.tx, bundle.jobRow.ID)
		require.NoError(t, err)
		return job
	}

	t.Run("RemoteCancellationViaCancel", func(t *testing.T) {
		t.Parallel()

		job := runCancelTest(t, errors.New("a non-nil error"))

		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, dbsqlc.JobStateCancelled, job.State)
		require.Len(t, job.Errors, 1)
		require.WithinDuration(t, time.Now(), job.Errors[0].At, 2*time.Second)
		require.Equal(t, uint16(1), job.Errors[0].Attempt)
		require.Equal(t, "jobCancelError: job cancelled remotely", job.Errors[0].Error)
		require.Equal(t, ErrJobCancelledRemotely.Error(), job.Errors[0].Error)
		require.Equal(t, "", job.Errors[0].Trace)
	})

	t.Run("RemoteCancellationJobNotCancelledIfNoErrorReturned", func(t *testing.T) {
		t.Parallel()

		job := runCancelTest(t, nil)

		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, dbsqlc.JobStateCompleted, job.State)
		require.Empty(t, job.Errors)
	})
}

func TestUnknownJobKindError_As(t *testing.T) {
	// This test isn't really necessary because we didn't have to write any code
	// to make it pass, but it does demonstrate that we can successfully use
	// errors.As with this custom error type.
	t.Parallel()

	t.Run("ReturnsTrueForAnotherUnregisteredKindError", func(t *testing.T) {
		t.Parallel()

		err1 := &UnknownJobKindError{Kind: "MyJobArgs"}
		var err2 *UnknownJobKindError
		require.ErrorAs(t, err1, &err2)
		require.Equal(t, err1, err2)
		require.Equal(t, err1.Kind, err2.Kind)
	})

	t.Run("ReturnsFalseForADifferentError", func(t *testing.T) {
		t.Parallel()

		var err *UnknownJobKindError
		require.False(t, errors.As(errors.New("some other error"), &err))
	})
}

func TestUnknownJobKindError_Is(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsTrueForAnotherUnregisteredKindError", func(t *testing.T) {
		t.Parallel()

		err1 := &UnknownJobKindError{Kind: "MyJobArgs"}
		require.ErrorIs(t, err1, &UnknownJobKindError{})
	})

	t.Run("ReturnsFalseForADifferentError", func(t *testing.T) {
		t.Parallel()

		err1 := &UnknownJobKindError{Kind: "MyJobArgs"}
		require.NotErrorIs(t, err1, errors.New("some other error"))
	})
}

func TestJobCancel(t *testing.T) {
	t.Parallel()

	t.Run("ErrorsIsReturnsTrueForAnotherErrorOfSameType", func(t *testing.T) {
		t.Parallel()
		err1 := JobCancel(errors.New("some message"))
		require.ErrorIs(t, err1, JobCancel(errors.New("another message")))
	})

	t.Run("ErrorsIsReturnsFalseForADifferentErrorType", func(t *testing.T) {
		t.Parallel()
		err1 := JobCancel(errors.New("some message"))
		require.NotErrorIs(t, err1, &UnknownJobKindError{Kind: "MyJobArgs"})
	})
}
