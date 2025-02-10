package jobexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/valutil"
	"github.com/riverqueue/river/rivertype"
)

type ClientRetryPolicy interface {
	NextRetry(job *rivertype.JobRow) time.Time
}

// ErrorHandler provides an interface that will be invoked in case of an error
// or panic occurring in the job. This is often useful for logging and exception
// tracking, but can also be used to customize retry behavior.
type ErrorHandler interface {
	// HandleError is invoked in case of an error occurring in a job.
	//
	// Context is descended from the one used to start the River client that
	// worked the job.
	HandleError(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult

	// HandlePanic is invoked in case of a panic occurring in a job.
	//
	// Context is descended from the one used to start the River client that
	// worked the job.
	HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult
}

type ErrorHandlerResult struct {
	// SetCancelled can be set to true to fail the job immediately and
	// permanently. By default it'll continue to follow the configured retry
	// schedule.
	SetCancelled bool
}

// Error used in CancelFunc in cases where the job was not cancelled for
// purposes of resource cleanup. Should never be user visible.
var errExecutorDefaultCancel = errors.New("context cancelled as executor finished")

type jobExecutorResult struct {
	Err        error
	NextRetry  time.Time
	PanicTrace string
	PanicVal   any
}

// ErrorStr returns an appropriate string to persist to the database based on
// the type of internal failure (i.e. error or panic). Panics if called on a
// non-errored result.
func (r *jobExecutorResult) ErrorStr() string {
	switch {
	case r.Err != nil:
		return r.Err.Error()
	case r.PanicVal != nil:
		return fmt.Sprintf("%v", r.PanicVal)
	}

	panic("ErrorStr should not be called on non-errored result")
}

type JobExecutor struct {
	baseservice.BaseService

	CancelFunc               context.CancelCauseFunc
	ClientJobTimeout         time.Duration
	Completer                jobcompleter.JobCompleter
	ClientRetryPolicy        ClientRetryPolicy
	DefaultClientRetryPolicy ClientRetryPolicy
	ErrorHandler             ErrorHandler
	InformProducerDoneFunc   func(jobRow *rivertype.JobRow)
	JobRow                   *rivertype.JobRow
	GlobalMiddleware         []rivertype.WorkerMiddleware
	SchedulerInterval        time.Duration
	WorkUnit                 workunit.WorkUnit

	// Meant to be used from within the job executor only.
	start time.Time
	stats *jobstats.JobStatistics // initialized by the executor, and handed off to completer
}

func (e *JobExecutor) Cancel() {
	e.Logger.Warn(e.Name+": job cancelled remotely", slog.Int64("job_id", e.JobRow.ID))
	e.CancelFunc(rivertype.ErrJobCancelledRemotely)
}

func (e *JobExecutor) Execute(ctx context.Context) {
	// Ensure that the context is cancelled no matter what, or it will leak:
	defer e.CancelFunc(errExecutorDefaultCancel)

	e.start = e.Time.NowUTC()
	e.stats = &jobstats.JobStatistics{
		QueueWaitDuration: e.start.Sub(e.JobRow.ScheduledAt),
	}

	res := e.execute(ctx)
	if res.Err != nil && errors.Is(context.Cause(ctx), rivertype.ErrJobCancelledRemotely) {
		res.Err = context.Cause(ctx)
	}

	e.reportResult(ctx, res)

	e.InformProducerDoneFunc(e.JobRow)
}

// Executes the job, handling a panic if necessary (and various other error
// conditions). The named return value is so that we can still return a value in
// case of a panic.
//
//nolint:nonamedreturns
func (e *JobExecutor) execute(ctx context.Context) (res *jobExecutorResult) {
	defer func() {
		if recovery := recover(); recovery != nil {
			e.Logger.ErrorContext(ctx, e.Name+": panic recovery; possible bug with Worker",
				slog.Int64("job_id", e.JobRow.ID),
				slog.String("kind", e.JobRow.Kind),
				slog.String("panic_val", fmt.Sprintf("%v", recovery)),
			)

			res = &jobExecutorResult{
				PanicTrace: string(debug.Stack()),
				PanicVal:   recovery,
			}
		}
		e.stats.RunDuration = e.Time.NowUTC().Sub(e.start)
	}()

	if e.WorkUnit == nil {
		e.Logger.ErrorContext(ctx, e.Name+": Unhandled job kind",
			slog.String("kind", e.JobRow.Kind),
			slog.Int64("job_id", e.JobRow.ID),
		)
		return &jobExecutorResult{Err: &rivertype.UnknownJobKindError{Kind: e.JobRow.Kind}}
	}

	if err := e.WorkUnit.UnmarshalJob(); err != nil {
		return &jobExecutorResult{Err: err}
	}

	doInner := execution.MiddlewareChain(e.GlobalMiddleware, e.WorkUnit.Middleware(), e.WorkUnit.Work, e.JobRow)
	jobTimeout := valutil.FirstNonZero(e.WorkUnit.Timeout(), e.ClientJobTimeout)
	ctx, cancel := execution.MaybeApplyTimeout(ctx, jobTimeout)
	defer cancel()

	return &jobExecutorResult{Err: doInner(ctx)}
}

func (e *JobExecutor) invokeErrorHandler(ctx context.Context, res *jobExecutorResult) bool {
	invokeAndHandlePanic := func(funcName string, errorHandler func() *ErrorHandlerResult) *ErrorHandlerResult {
		defer func() {
			if panicVal := recover(); panicVal != nil {
				e.Logger.ErrorContext(ctx, e.Name+": ErrorHandler invocation panicked",
					slog.String("function_name", funcName),
					slog.String("panic_val", fmt.Sprintf("%v", panicVal)),
				)
			}
		}()

		return errorHandler()
	}

	var errorHandlerRes *ErrorHandlerResult
	switch {
	case res.Err != nil:
		errorHandlerRes = invokeAndHandlePanic("HandleError", func() *ErrorHandlerResult {
			return e.ErrorHandler.HandleError(ctx, e.JobRow, res.Err)
		})

	case res.PanicVal != nil:
		errorHandlerRes = invokeAndHandlePanic("HandlePanic", func() *ErrorHandlerResult {
			return e.ErrorHandler.HandlePanic(ctx, e.JobRow, res.PanicVal, res.PanicTrace)
		})
	}

	return errorHandlerRes != nil && errorHandlerRes.SetCancelled
}

func (e *JobExecutor) reportResult(ctx context.Context, res *jobExecutorResult) {
	var snoozeErr *rivertype.JobSnoozeError

	if res.Err != nil && errors.As(res.Err, &snoozeErr) {
		e.Logger.DebugContext(ctx, e.Name+": Job snoozed",
			slog.Int64("job_id", e.JobRow.ID),
			slog.String("job_kind", e.JobRow.Kind),
			slog.Duration("duration", snoozeErr.Duration),
		)
		nextAttemptScheduledAt := time.Now().Add(snoozeErr.Duration)

		// Normally, snoozed jobs are set `scheduled` for the future and it's the
		// scheduler's job to set them back to `available` so they can be reworked.
		// Just as with retryable jobs, this isn't friendly for short snooze times
		// so we instead make the job immediately `available` if the snooze time is
		// smaller than the scheduler's run interval.
		var params *riverdriver.JobSetStateIfRunningParams
		if nextAttemptScheduledAt.Sub(e.Time.NowUTC()) <= e.SchedulerInterval {
			params = riverdriver.JobSetStateSnoozedAvailable(e.JobRow.ID, nextAttemptScheduledAt, e.JobRow.Attempt-1)
		} else {
			params = riverdriver.JobSetStateSnoozed(e.JobRow.ID, nextAttemptScheduledAt, e.JobRow.Attempt-1)
		}
		if err := e.Completer.JobSetStateIfRunning(ctx, e.stats, params); err != nil {
			e.Logger.ErrorContext(ctx, e.Name+": Error snoozing job",
				slog.Int64("job_id", e.JobRow.ID),
			)
		}
		return
	}

	if res.Err != nil || res.PanicVal != nil {
		e.reportError(ctx, res)
		return
	}

	if err := e.Completer.JobSetStateIfRunning(ctx, e.stats, riverdriver.JobSetStateCompleted(e.JobRow.ID, e.Time.NowUTC())); err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Error completing job",
			slog.String("err", err.Error()),
			slog.Int64("job_id", e.JobRow.ID),
		)
		return
	}
}

func (e *JobExecutor) reportError(ctx context.Context, res *jobExecutorResult) {
	var (
		cancelJob bool
		cancelErr *rivertype.JobCancelError
	)

	logAttrs := []any{
		slog.String("error", res.ErrorStr()),
		slog.Int64("job_id", e.JobRow.ID),
		slog.String("job_kind", e.JobRow.Kind),
	}

	switch {
	case errors.As(res.Err, &cancelErr):
		cancelJob = true
		e.Logger.DebugContext(ctx, e.Name+": Job cancelled explicitly", logAttrs...)
	case res.Err != nil:
		if e.JobRow.Attempt >= e.JobRow.MaxAttempts {
			e.Logger.ErrorContext(ctx, e.Name+": Job errored", logAttrs...)
		} else {
			e.Logger.WarnContext(ctx, e.Name+": Job errored; retrying", logAttrs...)
		}
	case res.PanicVal != nil:
		e.Logger.ErrorContext(ctx, e.Name+": Job panicked", logAttrs...)
	}

	if e.ErrorHandler != nil && !cancelJob {
		// Error handlers also have an opportunity to cancel the job.
		cancelJob = e.invokeErrorHandler(ctx, res)
	}

	attemptErr := rivertype.AttemptError{
		At:      e.start,
		Attempt: e.JobRow.Attempt,
		Error:   res.ErrorStr(),
		Trace:   res.PanicTrace,
	}

	errData, err := json.Marshal(attemptErr)
	if err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Failed to marshal attempt error", logAttrs...)
		return
	}

	now := time.Now()

	if cancelJob {
		if err := e.Completer.JobSetStateIfRunning(ctx, e.stats, riverdriver.JobSetStateCancelled(e.JobRow.ID, now, errData)); err != nil {
			e.Logger.ErrorContext(ctx, e.Name+": Failed to cancel job and report error", logAttrs...)
		}
		return
	}

	if e.JobRow.Attempt >= e.JobRow.MaxAttempts {
		if err := e.Completer.JobSetStateIfRunning(ctx, e.stats, riverdriver.JobSetStateDiscarded(e.JobRow.ID, now, errData)); err != nil {
			e.Logger.ErrorContext(ctx, e.Name+": Failed to discard job and report error", logAttrs...)
		}
		return
	}

	var nextRetryScheduledAt time.Time
	if e.WorkUnit != nil {
		nextRetryScheduledAt = e.WorkUnit.NextRetry()
	}
	if nextRetryScheduledAt.IsZero() {
		nextRetryScheduledAt = e.ClientRetryPolicy.NextRetry(e.JobRow)
	}
	if nextRetryScheduledAt.Before(now) {
		e.Logger.WarnContext(ctx,
			e.Name+": Retry policy returned invalid next retry before current time; using default retry policy instead",
			slog.Int("error_count", len(e.JobRow.Errors)+1),
			slog.Time("next_retry_scheduled_at", nextRetryScheduledAt),
			slog.Time("now", now),
		)
		nextRetryScheduledAt = e.DefaultClientRetryPolicy.NextRetry(e.JobRow)
	}

	// Normally, errored jobs are set `retryable` for the future and it's the
	// scheduler's job to set them back to `available` so they can be reworked.
	// This isn't friendly for smaller retry times though because it means that
	// effectively no retry time smaller than the scheduler's run interval is
	// respected. Here, we offset that with a branch that makes jobs immediately
	// `available` if their retry was smaller than the scheduler's run interval.
	var params *riverdriver.JobSetStateIfRunningParams
	if nextRetryScheduledAt.Sub(e.Time.NowUTC()) <= e.SchedulerInterval {
		params = riverdriver.JobSetStateErrorAvailable(e.JobRow.ID, nextRetryScheduledAt, errData)
	} else {
		params = riverdriver.JobSetStateErrorRetryable(e.JobRow.ID, nextRetryScheduledAt, errData)
	}
	if err := e.Completer.JobSetStateIfRunning(ctx, e.stats, params); err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Failed to report error for job", logAttrs...)
	}
}
