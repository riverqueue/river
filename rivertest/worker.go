package rivertest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/middlewarelookup"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// Worker makes it easier to test river workers. Once built, the worker can be
// used to insert and work any number jobs:
//
//	testWorker := rivertest.NewWorker(t, driver, config, worker)
//	result, err := testWorker.Work(ctx, t, tx, args, nil)
//	if err != nil {
//		t.Fatalf("failed to work job: %s", err)
//	}
//	if result.Kind != river.EventKindJobCompleted {
//		t.Fatalf("expected job to be completed, got %s", result.Kind)
//	}
//
// An existing job (inserted using external logic) can also be worked:
//
//	job := client.InsertTx(ctx, tx, args, nil)
//	// ...
//	result, err := worker.WorkJob(ctx, t, tx, job)
//	if err != nil {
//		t.Fatalf("failed to work job: %s", err)
//	}
//
// In all cases the underlying [river.Worker] will be called with the job
// transitioned into a running state. The execution environment has a realistic
// River context with access to all River features, including
// [river.ClientFromContext] and worker middleware.
type Worker[T river.JobArgs, TTx any] struct {
	client *river.Client[TTx]
	config *river.Config
	worker river.Worker[T]
}

// NewWorker creates a new test Worker for testing the provided [river.Worker].
// The worker uses the provided driver and River config to populate default
// values on test jobs and to configure the execution environment.
//
// A pool-less driver is recommended for most usage, as individual job inserts
// and executions will happen within a provided transaction. This enables many
// parallel test cases to run with full isolation, each in their own
// transaction.
//
// The worker is configured to disable unique enforcement by default, as this
// would otherwise prevent conflicting jobs from being tested in parallel.
func NewWorker[T river.JobArgs, TTx any](tb testing.TB, driver riverdriver.Driver[TTx], config *river.Config, worker river.Worker[T]) *Worker[T, TTx] {
	tb.Helper()

	config = config.WithDefaults()
	config.Test.DisableUniqueEnforcement = true

	client, err := river.NewClient(driver, config)
	if err != nil {
		tb.Fatalf("failed to create client: %s", err)
	}

	return &Worker[T, TTx]{
		client: client,
		config: config,
		worker: worker,
	}
}

func (w *Worker[T, TTx]) insertJob(ctx context.Context, tb testing.TB, tx TTx, args T, opts *river.InsertOpts) (*rivertype.JobRow, error) {
	tb.Helper()

	result, err := w.client.InsertTx(ctx, tx, args, opts)
	if err != nil {
		return nil, err
	}
	return result.Job, nil
}

// Work inserts a job with the provided arguments and optional insert options,
// then works it. The transaction is used for all work-related database
// operations so that the caller can easily roll back at the end of the test to
// maintain a clean database state.
//
// The returned WorkResult contains the updated job row from the database
// _after_ it has been worked, as well as the kind of event that occurred.
//
// The returned error only reflects _real_ errors and does not include
// explicitly returned snooze or cancel errors from the worker.
func (w *Worker[T, TTx]) Work(ctx context.Context, tb testing.TB, tx TTx, args T, opts *river.InsertOpts) (*WorkResult, error) {
	tb.Helper()

	job, err := w.insertJob(ctx, tb, tx, args, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job: %w", err)
	}
	return w.WorkJob(ctx, tb, tx, job)
}

// WorkJob works an existing job already in the database. The job must be
// inserted using external logic prior to calling this method. The transaction
// is used for all work-related database operations so that the caller can
// easily roll back at the end of the test to maintain a clean database state.
//
// The returned WorkResult contains the updated job row from the database
// _after_ it has been worked, as well as the kind of event that occurred.
//
// The returned error only reflects _real_ errors and does not include
// explicitly returned snooze or cancel errors from the worker.
func (w *Worker[T, TTx]) WorkJob(ctx context.Context, tb testing.TB, tx TTx, job *rivertype.JobRow) (*WorkResult, error) {
	tb.Helper()
	return w.workJob(ctx, tb, tx, job)
}

func (w *Worker[T, TTx]) workJob(ctx context.Context, tb testing.TB, tx TTx, job *rivertype.JobRow) (*WorkResult, error) {
	tb.Helper()

	timeGen := w.config.Test.Time
	if timeGen == nil {
		timeGen = &baseservice.UnStubbableTimeGenerator{}
	}

	exec := w.client.Driver().UnwrapExecutor(tx)
	subscribeCh := make(chan []jobcompleter.CompleterJobUpdated, 1)
	archetype := riversharedtest.BaseServiceArchetype(tb)
	if w.config.Logger != nil {
		archetype.Logger = w.config.Logger
	}
	if withStub, ok := timeGen.(baseservice.TimeGeneratorWithStub); ok {
		archetype.Time = withStub
	} else {
		archetype.Time = &baseservice.TimeGeneratorWithStubWrapper{TimeGenerator: timeGen}
	}
	completer := jobcompleter.NewInlineCompleter(archetype, w.config.Schema, exec, w.client.Pilot(), subscribeCh)

	for _, hook := range w.config.Hooks {
		if withBaseService, ok := hook.(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}
	for _, middleware := range w.config.Middleware {
		if withBaseService, ok := middleware.(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}

	updatedJobRow, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
		ID:                  job.ID,
		Attempt:             job.Attempt + 1,
		AttemptDoUpdate:     true,
		AttemptedAt:         ptrutil.Ptr(timeGen.NowUTC()),
		AttemptedAtDoUpdate: true,
		AttemptedBy:         append(job.AttemptedBy, w.config.ID),
		AttemptedByDoUpdate: true,
		StateDoUpdate:       true,
		State:               rivertype.JobStateRunning,
	})
	if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
		return nil, fmt.Errorf("test worker internal error: failed to update job to running state: %w", err)
	}
	job = updatedJobRow

	workUnit := (&workUnitFactoryWrapper[T]{worker: w.worker}).MakeUnit(job)

	// populate river client into context:
	ctx = WorkContext(ctx, w.client)
	// TODO: remove ContextKeyInsideTestWorker
	ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

	// jobCancel will always be called by the executor to prevent leaks.
	jobCtx, jobCancel := context.WithCancelCause(ctx)

	executionDone := make(chan struct{})

	var resultErr error

	executor := baseservice.Init(archetype, &jobexecutor.JobExecutor{
		CancelFunc:               jobCancel,
		ClientJobTimeout:         w.config.JobTimeout,
		ClientRetryPolicy:        w.config.RetryPolicy,
		Completer:                completer,
		DefaultClientRetryPolicy: &river.DefaultClientRetryPolicy{},
		ErrorHandler: &errorHandlerWrapper{
			HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *jobexecutor.ErrorHandlerResult {
				resultErr = err
				return nil
			},
			HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *jobexecutor.ErrorHandlerResult {
				resultErr = &PanicError{Cause: panicVal, Trace: trace}
				return nil
			},
		},
		InformProducerDoneFunc: func(job *rivertype.JobRow) { close(executionDone) },
		HookLookupGlobal:       hooklookup.NewHookLookup(w.config.Hooks),
		HookLookupByJob:        hooklookup.NewJobHookLookup(),
		JobRow:                 job,
		MiddlewareLookupGlobal: middlewarelookup.NewMiddlewareLookup(w.config.Middleware),
		SchedulerInterval:      maintenance.JobSchedulerIntervalDefault,
		WorkUnit:               workUnit,
	})

	executor.Execute(jobCtx)
	<-executionDone

	select {
	case completerResult := <-subscribeCh:
		if len(completerResult) == 0 {
			tb.Fatal("test worker internal error: empty job completion received")
		}
		if len(completerResult) > 1 {
			tb.Fatalf("test worker internal error: received %d job completions, expected 1", len(completerResult))
		}
		return completerResultToWorkResult(tb, completerResult[0]), resultErr
	default:
		tb.Fatal("test worker internal error: no job completions received")
	}
	panic("unreachable")
}

// WorkResult is the result of working a job in the test Worker.
type WorkResult struct {
	// EventKind is the kind of event that occurred following execution.
	EventKind river.EventKind

	// Job is the updated job row from the database _after_ it has been worked.
	Job *rivertype.JobRow
}

func completerResultToWorkResult(tb testing.TB, completerResult jobcompleter.CompleterJobUpdated) *WorkResult {
	tb.Helper()

	var kind river.EventKind
	switch completerResult.Job.State {
	case rivertype.JobStateCancelled:
		kind = river.EventKindJobCancelled
	case rivertype.JobStateCompleted:
		kind = river.EventKindJobCompleted
	case rivertype.JobStateScheduled:
		kind = river.EventKindJobSnoozed
	case rivertype.JobStateAvailable, rivertype.JobStateDiscarded, rivertype.JobStateRetryable, rivertype.JobStateRunning:
		kind = river.EventKindJobFailed
	case rivertype.JobStatePending:
		panic("test worker internal error: completion subscriber unexpectedly received job in pending state, river bug")
	default:
		// linter exhaustive rule prevents this from being reached
		panic("test worker internal error: unreachable state to distribute, river bug")
	}

	return &WorkResult{
		EventKind: kind,
		Job:       completerResult.Job,
	}
}

type PanicError struct {
	Cause any
	Trace string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("rivertest.PanicError: %v\n%s", e.Cause, e.Trace)
}

func (e *PanicError) Is(target error) bool {
	_, ok := target.(*PanicError)
	return ok
}

type errorHandlerWrapper struct {
	HandleErrorFunc func(ctx context.Context, job *rivertype.JobRow, err error) *jobexecutor.ErrorHandlerResult
	HandlePanicFunc func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *jobexecutor.ErrorHandlerResult
}

func (h *errorHandlerWrapper) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *jobexecutor.ErrorHandlerResult {
	return h.HandleErrorFunc(ctx, job, err)
}

func (h *errorHandlerWrapper) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *jobexecutor.ErrorHandlerResult {
	return h.HandlePanicFunc(ctx, job, panicVal, trace)
}
