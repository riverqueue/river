package rivertest

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/valutil"
	"github.com/riverqueue/river/rivertype"
)

// Worker makes it easier to test river workers. Once built, the worker can be
// used to work any number of synthetic jobs without touching the database:
//
//	worker := rivertest.NewWorker(t, driver, config, worker)
//	if err := worker.Work(ctx, t, args, nil); err != nil {
//		t.Fatalf("failed to work job: %s", err)
//	}
//
//	if err := worker.Work(ctx, t, args, &river.InsertOpts{Queue: "custom_queue"}); err != nil {
//		t.Fatalf("failed to work job: %s", err)
//	}
//
// In all cases the underlying [river.Worker] will be called with the synthetic
// job. The execution environment has a realistic River context with access to
// all River features, including [river.ClientFromContext] and worker
// middleware.
//
// When relying on features that require a database record (such as JobCompleteTx),
// the job must be inserted into the database first and then executed with
// WorkJob.
type Worker[T river.JobArgs, TTx any] struct {
	client *river.Client[TTx]
	config *river.Config
	worker river.Worker[T]
}

// NewWorker creates a new test Worker for testing the provided [river.Worker].
// The worker uses the provided driver and River config to populate default
// values on test jobs and to configure the execution environment.
//
// The database is not required to use this helper, and a pool-less driver is
// recommended for most usage. This enables individual test cases to run in
// parallel and with full isolation, even using a single shared `Worker`
// instance across many test cases.
func NewWorker[T river.JobArgs, TTx any](tb testing.TB, driver riverdriver.Driver[TTx], config *river.Config, worker river.Worker[T]) *Worker[T, TTx] {
	tb.Helper()

	config = config.WithDefaults()
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

// Work allocates a synthetic job with the provided arguments and optional
// insert options, then works it.
func (w *Worker[T, TTx]) Work(ctx context.Context, tb testing.TB, args T, opts *river.InsertOpts) error {
	tb.Helper()
	job := makeJobFromArgs(tb, w.config, args, opts)
	return w.WorkJob(ctx, tb, job)
}

// WorkTx allocates a synthetic job with the provided arguments and optional
// insert options, then works it in the given transaction.
func (w *Worker[T, TTx]) WorkTx(ctx context.Context, tb testing.TB, tx TTx, args T, opts *river.InsertOpts) error {
	tb.Helper()
	job := makeJobFromArgs(tb, w.config, args, opts)
	return w.WorkJobTx(ctx, tb, tx, job)
}

// WorkJob works the provided job. The job must be constructed to be a realistic
// job using external logic prior to calling this method. Unlike the other
// variants, this method offers total control over the job's attributes.
func (w *Worker[T, TTx]) WorkJob(ctx context.Context, tb testing.TB, job *river.Job[T]) error {
	tb.Helper()

	var exec riverdriver.Executor
	if w.client.Driver().HasPool() {
		exec = w.client.Driver().GetExecutor()
	}

	return w.workJobExec(ctx, tb, exec, job)
}

// WorkJobTx works the provided job in the given transaction. The job must be
// constructed to be a realistic job using external logic prior to calling this
// method. Unlike the other variants, this method offers total control over the
// job's attributes.
func (w *Worker[T, TTx]) WorkJobTx(ctx context.Context, tb testing.TB, tx TTx, job *river.Job[T]) error {
	tb.Helper()
	return w.workJobExec(ctx, tb, w.client.Driver().UnwrapExecutor(tx), job)
}

func (w *Worker[T, TTx]) workJobExec(ctx context.Context, tb testing.TB, exec riverdriver.Executor, job *river.Job[T]) error {
	tb.Helper()

	// Try to transition an existing job row to running, but also tolerate the
	// row not existing in the database. Most of the time you don't need to
	// insert a real job row to use this function (it's only necessary if you
	// want to use `JobCompleteTx`).
	if exec != nil {
		updatedJobRow, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
			ID:            job.ID,
			StateDoUpdate: true,
			State:         rivertype.JobStateRunning,
		})
		if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
			return err
		}
		if updatedJobRow != nil {
			job.JobRow = updatedJobRow
		}
	}

	// Regardless of whether the job was actually in the database, transition it
	// to running.
	job.JobRow.State = rivertype.JobStateRunning

	// populate river client into context:
	ctx = WorkContext(ctx, w.client)
	ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

	doInner := execution.MiddlewareChain(
		w.config.WorkerMiddleware,
		w.worker.Middleware(job),
		func(ctx context.Context) error { return w.worker.Work(ctx, job) },
		job.JobRow,
	)

	jobTimeout := valutil.FirstNonZero(w.worker.Timeout(job), w.config.JobTimeout)
	ctx, cancel := execution.MaybeApplyTimeout(ctx, jobTimeout)
	defer cancel()

	return doInner(ctx)
}

var idSeq int64 = 0 //nolint:gochecknoglobals

func nextID() int64 {
	return atomic.AddInt64(&idSeq, 1)
}

func makeJobFromArgs[T river.JobArgs](tb testing.TB, config *river.Config, args T, opts *river.InsertOpts) *river.Job[T] {
	tb.Helper()

	encodedArgs, err := json.Marshal(args)
	if err != nil {
		tb.Fatalf("failed to marshal args: %s", err)
	}

	// Round trip the args to validate JSON marshalling/unmarshalling on the type.
	var decodedArgs T
	if err = json.Unmarshal(encodedArgs, &decodedArgs); err != nil {
		tb.Fatalf("failed to unmarshal args: %s", err)
	}

	if opts == nil {
		opts = &river.InsertOpts{}
	}

	// Extract any job insert opts from args type if present
	var jobInsertOpts river.InsertOpts
	if argsWithOpts, ok := any(args).(river.JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}

	now := valutil.FirstNonZero(opts.ScheduledAt, jobInsertOpts.ScheduledAt, time.Now())

	internalUniqueOpts := (*dbunique.UniqueOpts)(&opts.UniqueOpts)
	insertParams := &rivertype.JobInsertParams{
		Args:        args,
		CreatedAt:   &now,
		EncodedArgs: encodedArgs,
		Kind:        args.Kind(),
		MaxAttempts: valutil.FirstNonZero(opts.MaxAttempts, jobInsertOpts.MaxAttempts, config.MaxAttempts, river.MaxAttemptsDefault),
		Metadata:    sliceutil.FirstNonEmpty(opts.Metadata, jobInsertOpts.Metadata, []byte(`{}`)),
		Priority:    valutil.FirstNonZero(opts.Priority, jobInsertOpts.Priority, river.PriorityDefault),
		Queue:       valutil.FirstNonZero(opts.Queue, jobInsertOpts.Queue, river.QueueDefault),
		State:       rivertype.JobStateAvailable,
		Tags:        sliceutil.FirstNonEmpty(opts.Tags, jobInsertOpts.Tags, []string{}),
	}
	var uniqueKey []byte
	timeGen := config.Test.Time
	if timeGen == nil {
		timeGen = &baseservice.UnStubbableTimeGenerator{}
	}
	if !internalUniqueOpts.IsEmpty() {
		uniqueKey, err = dbunique.UniqueKey(valutil.ValOrDefault(config.Test.Time, timeGen), internalUniqueOpts, insertParams)
		if err != nil {
			tb.Fatalf("failed to create unique key: %s", err)
		}
	}

	return makeJobFromFactoryBuild(tb, args, &testfactory.JobOpts{
		Attempt:      ptrutil.Ptr(1),
		AttemptedAt:  &now,
		AttemptedBy:  []string{"worker1"},
		CreatedAt:    &now,
		EncodedArgs:  encodedArgs,
		Errors:       nil,
		FinalizedAt:  nil,
		Kind:         ptrutil.Ptr(args.Kind()),
		MaxAttempts:  ptrutil.Ptr(valutil.FirstNonZero(opts.MaxAttempts, jobInsertOpts.MaxAttempts, config.MaxAttempts, river.MaxAttemptsDefault)),
		Metadata:     sliceutil.FirstNonEmpty(opts.Metadata, jobInsertOpts.Metadata, []byte(`{}`)),
		Priority:     ptrutil.Ptr(valutil.FirstNonZero(opts.Priority, jobInsertOpts.Priority, river.PriorityDefault)),
		Queue:        ptrutil.Ptr(valutil.FirstNonZero(opts.Queue, jobInsertOpts.Queue, river.QueueDefault)),
		ScheduledAt:  &now,
		State:        ptrutil.Ptr(rivertype.JobStateRunning),
		Tags:         sliceutil.FirstNonEmpty(opts.Tags, jobInsertOpts.Tags, []string{}),
		UniqueKey:    uniqueKey,
		UniqueStates: internalUniqueOpts.StateBitmask(),
	})
}

func makeJobFromFactoryBuild[T river.JobArgs](tb testing.TB, args T, jobOpts *testfactory.JobOpts) *river.Job[T] {
	tb.Helper()

	jobParams := testfactory.Job_Build(tb, jobOpts)

	var errors []rivertype.AttemptError
	if jobParams.Errors != nil {
		errors = make([]rivertype.AttemptError, len(jobParams.Errors))
		for i, err := range jobParams.Errors {
			var attemptError rivertype.AttemptError
			if err := json.Unmarshal(err, &attemptError); err != nil {
				tb.Fatalf("failed to unmarshal attempt error: %s", err)
			}
			errors[i] = attemptError
		}
	}

	now := time.Now()

	return &river.Job[T]{
		Args: args,
		JobRow: &rivertype.JobRow{
			ID:          nextID(),
			Attempt:     jobParams.Attempt,
			AttemptedAt: jobParams.AttemptedAt,
			AttemptedBy: jobParams.AttemptedBy,
			CreatedAt:   ptrutil.ValOrDefault(jobParams.CreatedAt, now),
			EncodedArgs: jobParams.EncodedArgs,
			Errors:      errors,
			FinalizedAt: jobParams.FinalizedAt,
			Kind:        jobParams.Kind,
			MaxAttempts: jobParams.MaxAttempts,
			Metadata:    jobParams.Metadata,
			Priority:    jobParams.Priority,
			Queue:       jobParams.Queue,
			ScheduledAt: ptrutil.ValOrDefault(jobParams.ScheduledAt, now),
			State:       jobParams.State,
			Tags:        jobParams.Tags,
			UniqueKey:   jobParams.UniqueKey,
		},
	}
}
