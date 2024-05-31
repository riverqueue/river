package river

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
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

	ctx := context.Background()

	type testBundle struct {
		completer    *jobcompleter.InlineCompleter
		exec         riverdriver.Executor
		errorHandler *testErrorHandler
		jobRow       *rivertype.JobRow
		updateCh     <-chan []jobcompleter.CompleterJobUpdated
	}

	setup := func(t *testing.T) (*jobExecutor, *testBundle) {
		t.Helper()

		var (
			tx        = riverinternaltest.TestTx(ctx, t)
			archetype = riverinternaltest.BaseServiceArchetype(t)
			exec      = riverpgxv5.New(nil).UnwrapExecutor(tx)
			updateCh  = make(chan []jobcompleter.CompleterJobUpdated, 10)
			completer = jobcompleter.NewInlineCompleter(archetype, exec, updateCh)
		)

		t.Cleanup(completer.Stop)

		workUnitFactory := newWorkUnitFactoryWithCustomRetry(func() error { return nil }, nil)

		job, err := exec.JobInsertFast(ctx, &riverdriver.JobInsertFastParams{
			EncodedArgs: []byte("{}"),
			Kind:        (callbackArgs{}).Kind(),
			MaxAttempts: rivercommon.MaxAttemptsDefault,
			Priority:    rivercommon.PriorityDefault,
			Queue:       rivercommon.QueueDefault,
			State:       rivertype.JobStateAvailable,
		})
		require.NoError(t, err)

		// Fetch the job to make sure it's marked as running:
		jobs, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			Max:   1,
			Queue: rivercommon.QueueDefault,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, job.ID, jobs[0].ID)
		job = jobs[0]

		bundle := &testBundle{
			completer:    completer,
			exec:         exec,
			errorHandler: newTestErrorHandler(),
			jobRow:       job,
			updateCh:     updateCh,
		}

		// allocate this context just so we can set the CancelFunc:
		_, cancel := context.WithCancelCause(ctx)
		t.Cleanup(func() { cancel(nil) })

		executor := baseservice.Init(archetype, &jobExecutor{
			CancelFunc:             cancel,
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
		jobUpdates := riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, job.State)

		require.Len(t, jobUpdates, 1)
		jobUpdate := jobUpdates[0]
		t.Logf("Job statistics: %+v", jobUpdate.JobStats)
		require.NotZero(t, jobUpdate.JobStats.CompleteDuration)
		require.NotZero(t, jobUpdate.JobStats.QueueWaitDuration)
		require.NotZero(t, jobUpdate.JobStats.RunDuration)

		select {
		case <-bundle.updateCh:
			t.Fatalf("unexpected job update: %+v", jobUpdate)
		default:
		}
	})

	t.Run("FirstError", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.Archetype.TimeNowUTC, _ = riverinternaltest.StubTime(time.Now().UTC())

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
		require.Len(t, job.Errors, 1)
		require.Equal(t, executor.Archetype.TimeNowUTC().Truncate(1*time.Microsecond), job.Errors[0].At.Truncate(1*time.Microsecond))
		require.Equal(t, 1, job.Errors[0].Attempt)
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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
	})

	t.Run("ErrorSetsJobAvailableBelowSchedulerIntervalThreshold", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.SchedulerInterval = 3 * time.Second

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		{
			executor.Execute(ctx)
			riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

			job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
			require.NoError(t, err)
			require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
			require.Equal(t, rivertype.JobStateAvailable, job.State)
		}

		_, err := bundle.exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
			ID:            bundle.jobRow.ID,
			StateDoUpdate: true,
			State:         rivertype.JobStateRunning,
		})
		require.NoError(t, err)

		bundle.jobRow.Attempt = 2

		{
			executor.Execute(ctx)
			riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

			job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
			require.NoError(t, err)
			require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 16*time.Second)
			require.Equal(t, rivertype.JobStateRetryable, job.State)
		}
	})

	t.Run("ErrorDiscardsJobAfterTooManyAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = bundle.jobRow.MaxAttempts

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateDiscarded, job.State)
	})

	t.Run("JobCancelErrorCancelsJobEvenWithRemainingAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		// ensure we still have remaining attempts:
		require.Greater(t, bundle.jobRow.MaxAttempts, bundle.jobRow.Attempt)

		cancelErr := JobCancel(errors.New("throw away this job"))
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateCancelled, job.State)
		require.Len(t, job.Errors, 1)
		require.WithinDuration(t, time.Now(), job.Errors[0].At, 2*time.Second)
		require.Equal(t, 1, job.Errors[0].Attempt)
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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, job.State)
		require.WithinDuration(t, time.Now().Add(30*time.Minute), job.ScheduledAt, 2*time.Second)
		require.Equal(t, maxAttemptsBefore+1, job.MaxAttempts)
		require.Empty(t, job.Errors)
	})

	t.Run("JobSnoozeErrorInNearFutureMakesJobAvailableAndIncrementsMaxAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		maxAttemptsBefore := bundle.jobRow.MaxAttempts

		cancelErr := JobSnooze(time.Millisecond)
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, job.State)
		require.WithinDuration(t, time.Now(), job.ScheduledAt, 2*time.Second)
		require.Equal(t, maxAttemptsBefore+1, job.MaxAttempts)
		require.Empty(t, job.Errors)
	})

	t.Run("ErrorWithCustomRetryPolicy", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.ClientRetryPolicy = &retryPolicyCustom{}

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
		require.WithinDuration(t, nextRetryAt, job.ScheduledAt, time.Microsecond)
	})

	t.Run("InvalidNextRetryAt", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.ClientRetryPolicy = &retryPolicyInvalid{}

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, (&DefaultClientRetryPolicy{}).NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)

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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, job.State)

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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandleErrorCalled)
	})

	t.Run("Panic", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
	})

	t.Run("PanicDiscardsJobAfterTooManyAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = bundle.jobRow.MaxAttempts

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateDiscarded, job.State)
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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)

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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, job.State)

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
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandlePanicCalled)
	})

	t.Run("CancelFuncCleanedUpEvenWithoutCancel", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return nil }, nil).MakeUnit(bundle.jobRow)

		workCtx, cancelFunc := context.WithCancelCause(ctx)
		executor.CancelFunc = cancelFunc

		executor.Execute(workCtx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		require.ErrorIs(t, context.Cause(workCtx), errExecutorDefaultCancel)
	})

	runCancelTest := func(t *testing.T, returnErr error) *rivertype.JobRow { //nolint:thelper
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
		t.Cleanup(func() { cancelFunc(nil) })

		executor.Execute(workCtx)
		riverinternaltest.WaitOrTimeout(t, bundle.updateCh)

		jobRow, err := bundle.exec.JobGetByID(ctx, bundle.jobRow.ID)
		require.NoError(t, err)
		return jobRow
	}

	t.Run("RemoteCancellationViaCancel", func(t *testing.T) {
		t.Parallel()

		job := runCancelTest(t, errors.New("a non-nil error"))

		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateCancelled, job.State)
		require.Len(t, job.Errors, 1)
		require.WithinDuration(t, time.Now(), job.Errors[0].At, 2*time.Second)
		require.Equal(t, 1, job.Errors[0].Attempt)
		require.Equal(t, "jobCancelError: job cancelled remotely", job.Errors[0].Error)
		require.Equal(t, ErrJobCancelledRemotely.Error(), job.Errors[0].Error)
		require.Equal(t, "", job.Errors[0].Trace)
	})

	t.Run("RemoteCancellationJobNotCancelledIfNoErrorReturned", func(t *testing.T) {
		t.Parallel()

		job := runCancelTest(t, nil)

		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateCompleted, job.State)
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
