package jobexecutor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/middlewarelookup"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/retrypolicytest"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// customizableWorkUnit is a wrapper around a workUnit that allows for customization
// of the workUnit.  Unlike in other packages, this one does not make use of any
// types from the top level river package (like `river.Job[T]`).
type customizableWorkUnit struct {
	middleware []rivertype.WorkerMiddleware
	nextRetry  func() time.Time
	timeout    time.Duration
	work       func() error
}

func (w *customizableWorkUnit) HookLookup(lookup *hooklookup.JobHookLookup) hooklookup.HookLookupInterface {
	return hooklookup.NewHookLookup(nil)
}

func (w *customizableWorkUnit) Middleware() []rivertype.WorkerMiddleware {
	return w.middleware
}

func (w *customizableWorkUnit) NextRetry() time.Time {
	if w.nextRetry != nil {
		return w.nextRetry()
	}
	return time.Time{}
}

func (w *customizableWorkUnit) Timeout() time.Duration {
	return w.timeout
}

func (w *customizableWorkUnit) UnmarshalJob() error {
	return nil
}

func (w *customizableWorkUnit) Work(ctx context.Context) error {
	return w.work()
}

type workUnitFactory struct {
	workUnit *customizableWorkUnit
}

func (w *workUnitFactory) MakeUnit(jobRow *rivertype.JobRow) workunit.WorkUnit {
	return w.workUnit
}

// Makes a workerInfo using the real workerWrapper with a job that uses a
// callback Work func and allows for customizable maxAttempts and nextRetry.
func newWorkUnitFactoryWithCustomRetry(f func() error, nextRetry func() time.Time) workunit.WorkUnitFactory {
	return &workUnitFactory{
		workUnit: &customizableWorkUnit{
			work:      f,
			nextRetry: nextRetry,
		},
	}
}

type testErrorHandler struct {
	HandleErrorCalled bool
	HandleErrorFunc   func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult

	HandlePanicCalled bool
	HandlePanicFunc   func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult
}

// Test handler with no-ops for both error handling functions.
func newTestErrorHandler() *testErrorHandler {
	return &testErrorHandler{
		HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult { return nil },
		HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
			return nil
		},
	}
}

func (h *testErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
	h.HandleErrorCalled = true
	return h.HandleErrorFunc(ctx, job, err)
}

func (h *testErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
	h.HandlePanicCalled = true
	return h.HandlePanicFunc(ctx, job, panicVal, trace)
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

	setup := func(t *testing.T) (*JobExecutor, *testBundle) {
		t.Helper()

		var (
			tx        = riverdbtest.TestTxPgx(ctx, t)
			archetype = riversharedtest.BaseServiceArchetype(t)
			exec      = riverpgxv5.New(nil).UnwrapExecutor(tx)
			updateCh  = make(chan []jobcompleter.CompleterJobUpdated, 10)
			completer = jobcompleter.NewInlineCompleter(archetype, "", exec, &riverpilot.StandardPilot{}, updateCh)
		)

		t.Cleanup(completer.Stop)

		workUnitFactory := newWorkUnitFactoryWithCustomRetry(func() error { return nil }, nil)

		now := time.Now().UTC()
		results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{
				{
					EncodedArgs: []byte("{}"),
					Kind:        "jobexecutor_test",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					// Needs to be explicitly set to a "now" horizon that's aligned with the
					// JobGetAvailable call. InsertMany applies a default scheduled_at in Go
					// so it can't pick up the Postgres-level `now()` default.
					ScheduledAt: ptrutil.Ptr(now),
					State:       rivertype.JobStateAvailable,
				},
			},
		})
		require.NoError(t, err)

		// Fetch the job to make sure it's marked as running:
		jobs, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			MaxToLock: 1,
			Now:       ptrutil.Ptr(now),
			Queue:     rivercommon.QueueDefault,
		})
		require.NoError(t, err)

		require.Len(t, jobs, 1)
		require.Equal(t, results[0].Job.ID, jobs[0].ID)
		job := jobs[0]

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

		executor := baseservice.Init(archetype, &JobExecutor{
			CancelFunc:               cancel,
			ClientRetryPolicy:        &retrypolicytest.RetryPolicyNoJitter{},
			Completer:                bundle.completer,
			DefaultClientRetryPolicy: &retrypolicytest.RetryPolicyNoJitter{},
			ErrorHandler:             bundle.errorHandler,
			HookLookupByJob:          hooklookup.NewJobHookLookup(),
			HookLookupGlobal:         hooklookup.NewHookLookup(nil),
			JobRow:                   bundle.jobRow,
			MiddlewareLookupGlobal:   middlewarelookup.NewMiddlewareLookup(nil),
			ProducerCallbacks: struct {
				JobDone func(jobRow *rivertype.JobRow)
				Stuck   func()
				Unstuck func()
			}{
				JobDone: func(jobRow *rivertype.JobRow) {},
				Stuck:   func() {},
				Unstuck: func() {},
			},
			SchedulerInterval: riverinternaltest.SchedulerShortInterval,
			WorkUnit:          workUnitFactory.MakeUnit(bundle.jobRow),
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
		jobUpdates := riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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

		now := executor.Time.StubNowUTC(time.Now().UTC())

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
		require.Len(t, job.Errors, 1)
		require.Equal(t, now.Truncate(1*time.Microsecond), job.Errors[0].At.Truncate(1*time.Microsecond))
		require.Equal(t, 1, job.Errors[0].Attempt)
		require.Equal(t, "job error", job.Errors[0].Error)
		require.Empty(t, job.Errors[0].Trace)
	})

	t.Run("ErrorAgainAfterRetry", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = 2

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
			riversharedtest.WaitOrTimeout(t, bundle.updateCh)

			job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
				ID:     bundle.jobRow.ID,
				Schema: "",
			})
			require.NoError(t, err)
			require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
			require.Equal(t, rivertype.JobStateAvailable, job.State)
		}

		_, err := bundle.exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			ID:            bundle.jobRow.ID,
			StateDoUpdate: true,
			State:         rivertype.JobStateRunning,
		})
		require.NoError(t, err)

		bundle.jobRow.Attempt = 2

		{
			executor.Execute(ctx)
			riversharedtest.WaitOrTimeout(t, bundle.updateCh)

			job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
				ID:     bundle.jobRow.ID,
				Schema: "",
			})
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateDiscarded, job.State)
	})

	t.Run("JobCancelErrorCancelsJobEvenWithRemainingAttempts", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		// ensure we still have remaining attempts:
		require.Greater(t, bundle.jobRow.MaxAttempts, bundle.jobRow.Attempt)

		// add a unique key so we can verify it's cleared
		var err error
		bundle.jobRow, err = bundle.exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			ID:    bundle.jobRow.ID,
			State: rivertype.JobStateAvailable, // required for encoding but ignored
		})
		require.NoError(t, err)

		cancelErr := rivertype.JobCancel(errors.New("throw away this job"))
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateCancelled, job.State)
		require.Nil(t, job.UniqueKey)
		require.Len(t, job.Errors, 1)
		require.WithinDuration(t, time.Now(), job.Errors[0].At, 2*time.Second)
		require.Equal(t, 1, job.Errors[0].Attempt)
		require.Equal(t, "JobCancelError: throw away this job", job.Errors[0].Error)
		require.Empty(t, job.Errors[0].Trace)
	})

	t.Run("JobSnoozeErrorReschedulesJobAndDecrementsAttempt", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		attemptBefore := bundle.jobRow.Attempt

		cancelErr := &rivertype.JobSnoozeError{Duration: 30 * time.Minute}
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, job.State)
		require.WithinDuration(t, time.Now().Add(30*time.Minute), job.ScheduledAt, 2*time.Second)
		require.Equal(t, attemptBefore-1, job.Attempt)
		require.Empty(t, job.Errors)
	})

	t.Run("JobSnoozeErrorInNearFutureMakesJobAvailableAndDecrementsAttempt", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		attemptBefore := bundle.jobRow.Attempt

		cancelErr := &rivertype.JobSnoozeError{Duration: time.Millisecond}
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return cancelErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, job.State)
		require.WithinDuration(t, time.Now(), job.ScheduledAt, 2*time.Second)
		require.Equal(t, attemptBefore-1, job.Attempt)
		require.Empty(t, job.Errors)
	})

	t.Run("ErrorWithCustomRetryPolicy", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.ClientRetryPolicy = &retrypolicytest.RetryPolicyCustom{}

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
		require.WithinDuration(t, nextRetryAt, job.ScheduledAt, time.Microsecond)
	})

	t.Run("InvalidNextRetryAt", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.ClientRetryPolicy = &retrypolicytest.RetryPolicyInvalid{}

		workerErr := errors.New("job error")
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return workerErr }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.WithinDuration(t, executor.DefaultClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandleErrorCalled)
	})

	t.Run("ExpandableErrorsApplyToMultipleJobsIndividually", func(t *testing.T) {
		makeExtraRunningJobs := func(t *testing.T, exec riverdriver.Executor) []*rivertype.JobRow {
			t.Helper()

			now := time.Now().UTC()
			_, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
				Jobs: []*riverdriver.JobInsertFullParams{
					testfactory.Job_Build(t, &testfactory.JobOpts{Kind: ptrutil.Ptr("jobexecutor_test"), ScheduledAt: &now}),
					testfactory.Job_Build(t, &testfactory.JobOpts{Kind: ptrutil.Ptr("jobexecutor_test"), ScheduledAt: &now}),
					testfactory.Job_Build(t, &testfactory.JobOpts{Kind: ptrutil.Ptr("jobexecutor_test"), ScheduledAt: &now}),
				},
			})
			require.NoError(t, err)

			locked, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				MaxToLock: 3,
				Now:       &now,
				Queue:     rivercommon.QueueDefault,
			})
			require.NoError(t, err)
			require.Len(t, locked, 3)
			return locked
		}

		t.Run("AllJobsShareSameNormalError", func(t *testing.T) {
			t.Parallel()

			executor, bundle := setup(t)
			allJobs := append([]*rivertype.JobRow{bundle.jobRow}, makeExtraRunningJobs(t, bundle.exec)...)

			errAll := errors.New("job error")
			perJob := map[int64]error{
				allJobs[0].ID: errAll,
				allJobs[1].ID: errAll,
				allJobs[2].ID: errAll,
				allJobs[3].ID: errAll,
			}
			executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
				return &errorBundle{errorsByID: perJob, jobs: allJobs}
			}, nil).MakeUnit(bundle.jobRow)

			executor.Execute(ctx)
			riversharedtest.WaitOrTimeoutN(t, bundle.updateCh, len(allJobs))

			for i, j := range allJobs {
				got, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: j.ID, Schema: ""})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateRetryable, got.State, "expected job %d to be retryable, got %s", i, got.State)
				require.Len(t, got.Errors, 1)
				require.Equal(t, "job error", got.Errors[0].Error)
			}
		})

		t.Run("OnlySomeJobsHaveErrors", func(t *testing.T) {
			t.Parallel()

			executor, bundle := setup(t)
			allJobs := append([]*rivertype.JobRow{bundle.jobRow}, makeExtraRunningJobs(t, bundle.exec)...)

			errSome := errors.New("job error")
			perJob := map[int64]error{
				allJobs[0].ID: errSome,
				// allJobs[1] omitted -> success
				allJobs[2].ID: errSome,
				allJobs[3].ID: nil, // explicit nil: success
			}
			executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
				return &errorBundle{errorsByID: perJob, jobs: allJobs}
			}, nil).MakeUnit(bundle.jobRow)

			executor.Execute(ctx)
			riversharedtest.WaitOrTimeoutN(t, bundle.updateCh, len(allJobs))

			// errored jobs -> retryable with 1 error
			for _, idx := range []int{0, 2} {
				got, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: allJobs[idx].ID, Schema: ""})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateRetryable, got.State)
				require.Len(t, got.Errors, 1)
				require.Equal(t, "job error", got.Errors[0].Error)
			}

			// successful jobs -> completed, no errors
			for _, idx := range []int{1, 3} {
				got, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: allJobs[idx].ID, Schema: ""})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateCompleted, got.State)
				require.Empty(t, got.Errors)
			}
		})

		t.Run("AllJobsHaveErrorsViaBundle", func(t *testing.T) {
			t.Parallel()

			executor, bundle := setup(t)
			allJobs := append([]*rivertype.JobRow{bundle.jobRow}, makeExtraRunningJobs(t, bundle.exec)...)

			perJob := map[int64]error{
				allJobs[0].ID: errors.New("job error 0"),
				allJobs[1].ID: errors.New("job error 1"),
				allJobs[2].ID: errors.New("job error 2"),
				allJobs[3].ID: errors.New("job error 3"),
			}
			executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
				return &errorBundle{errorsByID: perJob, jobs: allJobs}
			}, nil).MakeUnit(bundle.jobRow)

			executor.Execute(ctx)
			riversharedtest.WaitOrTimeoutN(t, bundle.updateCh, len(allJobs))

			for i, j := range allJobs {
				got, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: j.ID, Schema: ""})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateRetryable, got.State)
				require.Len(t, got.Errors, 1)
				require.Equal(t, fmt.Sprintf("job error %d", i), got.Errors[0].Error)
			}
		})
	})

	configureStuckDetection := func(executor *JobExecutor) {
		executor.ClientJobTimeout = 5 * time.Millisecond
		executor.StuckThresholdOverride = 1 * time.Nanosecond // must be greater than 0 to take effect
	}

	t.Run("StuckDetectionActivatesForClientTimeout", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		configureStuckDetection(executor)

		var (
			informProducerStuckReceived   = make(chan struct{})
			informProducerUnstuckReceived = make(chan struct{})
		)
		executor.ProducerCallbacks.Stuck = func() {
			t.Log("Job executor reported stuck")
			close(informProducerStuckReceived)
		}
		executor.ProducerCallbacks.Unstuck = func() {
			t.Log("Job executor reported unstuck (after being stuck)")
			close(informProducerUnstuckReceived)
		}

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			riversharedtest.WaitOrTimeout(t, informProducerStuckReceived)

			select {
			case <-informProducerUnstuckReceived:
				require.FailNow(t, "Executor should not have reported unstuck immediately")
			case <-time.After(10 * time.Millisecond):
				t.Log("Job executor still stuck after wait (this is expected)")
			}

			return nil
		}, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		_ = riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		riversharedtest.WaitOrTimeout(t, informProducerUnstuckReceived)
	})

	t.Run("StuckDetectionActivatesForWorkerTimeout", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		// Does not use configureStuckDetection to avoid ClientJobTimeout being
		// set. Instead, customizableWorkUnit.timeout is set below instead.
		executor.StuckThresholdOverride = 1 * time.Nanosecond // must be greater than 0 to take effect

		var (
			informProducerStuckReceived   = make(chan struct{})
			informProducerUnstuckReceived = make(chan struct{})
		)
		executor.ProducerCallbacks.Stuck = func() {
			t.Log("Job executor reported stuck")
			close(informProducerStuckReceived)
		}
		executor.ProducerCallbacks.Unstuck = func() {
			t.Log("Job executor reported unstuck (after being stuck)")
			close(informProducerUnstuckReceived)
		}

		workUnit := &customizableWorkUnit{
			timeout: 5 * time.Millisecond,
			work: func() error {
				riversharedtest.WaitOrTimeout(t, informProducerStuckReceived)

				select {
				case <-informProducerUnstuckReceived:
					require.FailNow(t, "Executor should not have reported unstuck immediately")
				case <-time.After(10 * time.Millisecond):
					t.Log("Job executor still stuck after wait (this is expected)")
				}

				return nil
			},
		}

		executor.WorkUnit = (&workUnitFactory{
			workUnit: workUnit,
		}).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		_ = riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		riversharedtest.WaitOrTimeout(t, informProducerUnstuckReceived)
	})

	// Checks that even if a work context is cancelled immediately, stuck
	// detection still works as expected.
	t.Run("StuckDetectionIgnoresParentContextCancellation", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		configureStuckDetection(executor)

		var (
			informProducerStuckReceived   = make(chan struct{})
			informProducerUnstuckReceived = make(chan struct{})
		)
		executor.ProducerCallbacks.Stuck = func() {
			t.Log("Job executor reported stuck")
			close(informProducerStuckReceived)
		}
		executor.ProducerCallbacks.Unstuck = func() {
			t.Log("Job executor reported unstuck (after being stuck)")
			close(informProducerUnstuckReceived)
		}

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			riversharedtest.WaitOrTimeout(t, informProducerStuckReceived)

			select {
			case <-informProducerUnstuckReceived:
				require.FailNow(t, "Executor should not have reported unstuck immediately")
			case <-time.After(10 * time.Millisecond):
				t.Log("Job executor still stuck after wait (this is expected)")
			}

			return nil
		}, nil).MakeUnit(bundle.jobRow)

		ctx, cancel := context.WithCancel(ctx)
		cancel() // cancel immediately

		executor.Execute(ctx)
		_ = riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		riversharedtest.WaitOrTimeout(t, informProducerUnstuckReceived)
	})

	t.Run("Panic", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.WithinDuration(t, executor.ClientRetryPolicy.NextRetry(bundle.jobRow), job.ScheduledAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateRetryable, job.State)
		require.Len(t, job.Errors, 1)
		// Sufficient enough to ensure that the stack trace is included:
		require.Contains(t, job.Errors[0].Trace, "river/internal/jobexecutor/job_executor.go")
	})

	t.Run("PanicAgainAfterRetry", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		bundle.jobRow.Attempt = 2

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 1*time.Second)
		require.Equal(t, rivertype.JobStateDiscarded, job.State)
	})

	t.Run("PanicWithPanicHandler", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		// Add a middleware so we can verify it's in the trace too:
		executor.MiddlewareLookupGlobal = middlewarelookup.NewMiddlewareLookup([]rivertype.Middleware{
			&testMiddleware{
				work: func(ctx context.Context, job *rivertype.JobRow, next func(context.Context) error) error {
					return next(ctx)
				},
			},
		})

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			panic("panic val")
		}, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandlePanicFunc = func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
			require.Equal(t, "panic val", panicVal)
			require.NotContains(t, trace, "runtime/debug.Stack()\n")
			require.Contains(t, trace, "(*testMiddleware).Work")
			// Ensure that the first frame (i.e. the file and line info) corresponds
			// to the code that raised the panic. This ensures we've stripped out
			// irrelevant frames like the ones from the runtime package which
			// generated the trace, or the panic rescuing code.
			lines := strings.Split(trace, "\n")
			require.GreaterOrEqual(t, len(lines), 2, "expected at least one frame in the stack trace")
			firstFrame := lines[1] // this line contains the file and line of the panic origin
			require.Contains(t, firstFrame, "job_executor_test.go")

			return nil
		}

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRetryable, job.State)

		require.True(t, bundle.errorHandler.HandlePanicCalled)
	})

	t.Run("PanicWithPanicHandlerSetCancelled", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandlePanicFunc = func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
			return &ErrorHandlerResult{SetCancelled: true}
		}

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, job.State)

		require.True(t, bundle.errorHandler.HandlePanicCalled)
	})

	t.Run("PanicWithPanicHandlerPanic", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { panic("panic val") }, nil).MakeUnit(bundle.jobRow)
		bundle.errorHandler.HandlePanicFunc = func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
			panic("panic handler panicked!")
		}

		executor.Execute(ctx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

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
			executor.Cancel(ctx)
			close(haveCancelled)
		}()

		workCtx, cancelFunc := context.WithCancelCause(ctx)
		executor.CancelFunc = cancelFunc
		t.Cleanup(func() { cancelFunc(nil) })

		executor.Execute(workCtx)
		riversharedtest.WaitOrTimeout(t, bundle.updateCh)

		jobRow, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     bundle.jobRow.ID,
			Schema: "",
		})
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
		require.Equal(t, "JobCancelError: job cancelled remotely", job.Errors[0].Error)
		require.Equal(t, rivertype.ErrJobCancelledRemotely.Error(), job.Errors[0].Error)
		require.Empty(t, job.Errors[0].Trace)
	})

	t.Run("RemoteCancellationJobNotCancelledIfNoErrorReturned", func(t *testing.T) {
		t.Parallel()

		job := runCancelTest(t, nil)

		require.WithinDuration(t, time.Now(), *job.FinalizedAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateCompleted, job.State)
		require.Empty(t, job.Errors)
	})

	t.Run("WorkHooks", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		var (
			workBeginCalled bool
			workEndCalled   bool
		)
		executor.HookLookupGlobal = hooklookup.NewHookLookup([]rivertype.Hook{
			HookWorkBeginFunc(func(ctx context.Context, job *rivertype.JobRow) error {
				workBeginCalled = true
				return nil
			}),
			HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
				workEndCalled = true
				return err
			}),
		})
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error { return nil }, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		jobsUpdated := riversharedtest.WaitOrTimeout(t, bundle.updateCh)
		require.Len(t, jobsUpdated, 1)
		require.Empty(t, jobsUpdated[0].Job.Errors)

		require.True(t, workBeginCalled)
		require.True(t, workEndCalled)
	})

	t.Run("WorkEndErrorPassThrough", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		var (
			workEnd1Called bool
			workEnd2Called bool
		)
		executor.HookLookupGlobal = hooklookup.NewHookLookup([]rivertype.Hook{
			HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
				workEnd1Called = true
				require.EqualError(t, err, "job error")
				return err
			}),
			HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
				workEnd2Called = true
				require.EqualError(t, err, "job error")
				return err
			}),
		})
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			return errors.New("job error")
		}, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		jobsUpdated := riversharedtest.WaitOrTimeout(t, bundle.updateCh)
		require.Len(t, jobsUpdated, 1)
		require.Equal(t, "job error", jobsUpdated[0].Job.Errors[0].Error)

		require.True(t, workEnd1Called)
		require.True(t, workEnd2Called)
	})

	t.Run("WorkEndErrorSuppression", func(t *testing.T) {
		t.Parallel()

		executor, bundle := setup(t)

		var (
			workEnd1Called bool
			workEnd2Called bool
		)
		executor.HookLookupGlobal = hooklookup.NewHookLookup([]rivertype.Hook{
			HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
				workEnd1Called = true
				require.EqualError(t, err, "job error")
				return err
			}),
			HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
				workEnd2Called = true
				require.EqualError(t, err, "job error")
				return nil // second hook suppresses the error
			}),
		})
		executor.WorkUnit = newWorkUnitFactoryWithCustomRetry(func() error {
			return errors.New("job error")
		}, nil).MakeUnit(bundle.jobRow)

		executor.Execute(ctx)
		jobsUpdated := riversharedtest.WaitOrTimeout(t, bundle.updateCh)
		require.Len(t, jobsUpdated, 1)
		require.Empty(t, jobsUpdated[0].Job.Errors)

		require.True(t, workEnd1Called)
		require.True(t, workEnd2Called)
	})
}

//
// *Func types are copied from the top level River package because they can't be
// accessed from here.
//

type HookWorkBeginFunc func(ctx context.Context, job *rivertype.JobRow) error

func (f HookWorkBeginFunc) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	return f(ctx, job)
}

func (f HookWorkBeginFunc) IsHook() bool { return true }

type HookWorkEndFunc func(ctx context.Context, job *rivertype.JobRow, err error) error

func (f HookWorkEndFunc) WorkEnd(ctx context.Context, job *rivertype.JobRow, err error) error {
	return f(ctx, job, err)
}

func (f HookWorkEndFunc) IsHook() bool { return true }

type testMiddleware struct {
	work func(ctx context.Context, job *rivertype.JobRow, next func(context.Context) error) error
}

func (m *testMiddleware) IsMiddleware() bool { return true }

func (m *testMiddleware) Work(ctx context.Context, job *rivertype.JobRow, next func(context.Context) error) error {
	return m.work(ctx, job, next)
}

// errorBundle is for testing withJobAndErrorsByID.
type errorBundle struct { //nolint:errname
	errorsByID map[int64]error
	jobs       []*rivertype.JobRow
}

func (eb *errorBundle) Error() string { return "error bundle" }

func (eb *errorBundle) ErrorsByID() map[int64]error { return eb.errorsByID }
func (eb *errorBundle) Jobs() []*rivertype.JobRow   { return eb.jobs }

var _ withJobsAndErrorsByID = (*errorBundle)(nil)
