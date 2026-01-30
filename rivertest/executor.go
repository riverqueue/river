package rivertest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

// Executor can execute any job kind registered on a River client.
type Executor[TTx any] struct {
	client *river.Client[TTx]
	config *river.Config

	afterWork func(ctx context.Context, tx TTx, result *WorkResult) error
}

// ExecutorOpts are options for NewExecutorOpts.
type ExecutorOpts[TTx any] struct {
	// AfterWork runs after job execution completes.
	AfterWork func(ctx context.Context, tx TTx, result *WorkResult) error

	// Client is an optional preconstructed River client. If provided, Driver
	// is ignored and Config must be non-nil.
	Client *river.Client[TTx]

	// Config configures the test executor. It is required when Client is
	// provided.
	Config *river.Config

	// Driver is required when Client is not provided.
	Driver riverdriver.Driver[TTx]
}

// NewExecutorOpts creates a new test Executor using the provided options.
// It supports using an existing River client via ExecutorOpts.Client.
func NewExecutorOpts[TTx any](tb testing.TB, opts *ExecutorOpts[TTx]) *Executor[TTx] {
	tb.Helper()

	if opts == nil {
		opts = &ExecutorOpts[TTx]{}
	}

	if opts.Client != nil && opts.Config == nil {
		tb.Fatalf("rivertest.NewExecutorOpts: Config must be set when providing Client")
	}

	if opts.Client == nil && opts.Driver == nil {
		tb.Fatalf("rivertest.NewExecutorOpts: Driver must be set when Client is not provided")
	}

	config := opts.Config.WithDefaults()
	config.Test.DisableUniqueEnforcement = true

	var (
		client *river.Client[TTx]
		err    error
	)
	if opts.Client != nil {
		client = opts.Client
	} else {
		client, err = river.NewClient(opts.Driver, config)
		if err != nil {
			tb.Fatalf("failed to create client: %s", err)
		}
	}

	return &Executor[TTx]{
		client: client,
		config: config,

		afterWork: opts.AfterWork,
	}
}

func (e *Executor[TTx]) insertJob(ctx context.Context, tb testing.TB, tx TTx, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobRow, error) {
	tb.Helper()

	result, err := e.client.InsertTx(ctx, tx, args, opts)
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
func (e *Executor[TTx]) Work(ctx context.Context, tb testing.TB, tx TTx, args river.JobArgs, opts *river.InsertOpts) (*WorkResult, error) {
	tb.Helper()

	job, err := e.insertJob(ctx, tb, tx, args, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job: %w", err)
	}
	return e.WorkJob(ctx, tb, tx, job)
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
func (e *Executor[TTx]) WorkJob(ctx context.Context, tb testing.TB, tx TTx, job *rivertype.JobRow) (*WorkResult, error) {
	tb.Helper()
	return e.workJob(ctx, tb, tx, job)
}

func (e *Executor[TTx]) workJob(ctx context.Context, tb testing.TB, tx TTx, job *rivertype.JobRow) (*WorkResult, error) {
	tb.Helper()

	timeGen := e.config.Test.Time
	if timeGen == nil {
		timeGen = &baseservice.UnStubbableTimeGenerator{}
	}

	exec := e.client.Driver().UnwrapExecutor(tx)
	subscribeCh := make(chan []jobcompleter.CompleterJobUpdated, 1)
	archetype := riversharedtest.BaseServiceArchetype(tb)
	if e.config.Logger != nil {
		archetype.Logger = e.config.Logger
	}
	if withStub, ok := timeGen.(baseservice.TimeGeneratorWithStub); ok {
		archetype.Time = withStub
	} else {
		archetype.Time = &baseservice.TimeGeneratorWithStubWrapper{TimeGenerator: timeGen}
	}
	completer := jobcompleter.NewInlineCompleter(archetype, e.config.Schema, exec, e.client.Pilot(), subscribeCh)

	for _, hook := range e.config.Hooks {
		if withBaseService, ok := hook.(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}
	for _, middleware := range e.config.Middleware {
		if withBaseService, ok := middleware.(baseservice.WithBaseService); ok {
			baseservice.Init(archetype, withBaseService)
		}
	}

	updatedJobRow, err := exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
		ID:                  job.ID,
		Attempt:             job.Attempt + 1,
		AttemptDoUpdate:     true,
		AttemptedAt:         ptrutil.Ptr(timeGen.NowUTC()),
		AttemptedAtDoUpdate: true,
		AttemptedBy:         append(job.AttemptedBy, e.config.ID),
		AttemptedByDoUpdate: true,
		StateDoUpdate:       true,
		State:               rivertype.JobStateRunning,
	})
	if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
		return nil, fmt.Errorf("test executor internal error: failed to update job to running state: %w", err)
	}
	job = updatedJobRow

	if e.config.Workers == nil {
		return nil, errors.New("test executor internal error: no workers configured on client")
	}
	factoryFunc, found := e.config.Workers.LookupWorkUnitFactory(job.Kind)
	if !found {
		return nil, fmt.Errorf("test executor internal error: no worker registered for kind %q", job.Kind)
	}
	workUnitAny := factoryFunc(job)
	workUnit, isWorkUnit := workUnitAny.(workunit.WorkUnit)
	if !isWorkUnit {
		return nil, fmt.Errorf("test executor internal error: work unit factory for kind %q returned unexpected type", job.Kind)
	}

	// populate river client into context:
	ctx = WorkContext(ctx, e.client)
	// TODO: remove ContextKeyInsideTestWorker
	ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

	// jobCancel will always be called by the executor to prevent leaks.
	jobCtx, jobCancel := context.WithCancelCause(ctx)

	executionDone := make(chan struct{})

	var resultErr error

	executor := newTestJobExecutor(archetype, e.config, completer, job, workUnit, jobCancel, executionDone, &resultErr)

	executor.Execute(jobCtx)
	<-executionDone

	select {
	case completerResult := <-subscribeCh:
		if len(completerResult) == 0 {
			tb.Fatal("test executor internal error: empty job completion received")
		}
		if len(completerResult) > 1 {
			tb.Fatalf("test executor internal error: received %d job completions, expected 1", len(completerResult))
		}
		result := completerResultToWorkResult(tb, completerResult[0])
		var afterWorkErr error
		if e.afterWork != nil {
			afterWorkErr = e.afterWork(ctx, tx, result)
		}
		if afterWorkErr != nil {
			afterWorkErr = fmt.Errorf("after work hook: %w", afterWorkErr)
		}
		if resultErr != nil || afterWorkErr != nil {
			return result, errors.Join(resultErr, afterWorkErr)
		}
		return result, nil
	default:
		tb.Fatal("test executor internal error: no job completions received")
	}
	panic("unreachable")
}
