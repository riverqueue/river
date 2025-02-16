package rivertest

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
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

func (w *Worker[T, TTx]) insertJob(ctx context.Context, tb testing.TB, tx TTx, args T, opts *river.InsertOpts) *river.Job[T] {
	tb.Helper()

	result, err := w.client.InsertTx(ctx, tx, args, opts)
	if err != nil {
		tb.Fatalf("failed to insert job: %s", err)
	}
	var decodedArgs T
	if err := json.Unmarshal(result.Job.EncodedArgs, &decodedArgs); err != nil {
		tb.Fatalf("failed to unmarshal args: %s", err)
	}

	return &river.Job[T]{
		Args:   decodedArgs,
		JobRow: result.Job,
	}
}

// Work allocates a synthetic job with the provided arguments and optional
// insert options, then works it.
func (w *Worker[T, TTx]) Work(ctx context.Context, tb testing.TB, tx TTx, args T, opts *river.InsertOpts) error {
	tb.Helper()

	job := w.insertJob(ctx, tb, tx, args, opts)
	return w.WorkJob(ctx, tb, tx, job)
}

// WorkTx allocates a synthetic job with the provided arguments and optional
// insert options, then works it in the given transaction.
func (w *Worker[T, TTx]) WorkTx(ctx context.Context, tb testing.TB, tx TTx, args T, opts *river.InsertOpts) error {
	tb.Helper()

	job := w.insertJob(ctx, tb, tx, args, opts)
	return w.WorkJobTx(ctx, tb, tx, job)
}

// WorkJob works the provided job. The job must be constructed to be a realistic
// job using external logic prior to calling this method. Unlike the other
// variants, this method offers total control over the job's attributes.
func (w *Worker[T, TTx]) WorkJob(ctx context.Context, tb testing.TB, tx TTx, job *river.Job[T]) error {
	tb.Helper()
	return w.workJob(ctx, tb, tx, job)
}

// WorkJobTx works the provided job in the given transaction. The job must be
// constructed to be a realistic job using external logic prior to calling this
// method. Unlike the other variants, this method offers total control over the
// job's attributes.
func (w *Worker[T, TTx]) WorkJobTx(ctx context.Context, tb testing.TB, tx TTx, job *river.Job[T]) error {
	tb.Helper()
	return w.workJob(ctx, tb, tx, job)
}

func (w *Worker[T, TTx]) workJob(ctx context.Context, tb testing.TB, tx TTx, job *river.Job[T]) error {
	tb.Helper()

	exec := w.client.Driver().UnwrapExecutor(tx)

	// Try to transition an existing job row to running, but also tolerate the
	// row not existing in the database. Most of the time you don't need to
	// insert a real job row to use this function (it's only necessary if you
	// want to use `JobCompleteTx`).
	if exec != nil {
		timeGen := w.config.Test.Time
		if timeGen == nil {
			timeGen = &baseservice.UnStubbableTimeGenerator{}
		}
		updatedJobRow, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
			ID:                  job.ID,
			Attempt:             job.JobRow.Attempt + 1,
			AttemptDoUpdate:     true,
			AttemptedAt:         ptrutil.Ptr(timeGen.NowUTC()),
			AttemptedAtDoUpdate: true,
			AttemptedBy:         append(job.JobRow.AttemptedBy, "worker1"),
			AttemptedByDoUpdate: true,
			StateDoUpdate:       true,
			State:               rivertype.JobStateRunning,
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
