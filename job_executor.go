package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// Error used in CancelFunc in cases where the job was not cancelled for
// purposes of resource cleanup. Should never be user visible.
var errExecutorDefaultCancel = errors.New("context cancelled as executor finished")

// UnknownJobKindError is returned when a Client fetches and attempts to
// work a job that has not been registered on the Client's Workers bundle (using
// AddWorker).
type UnknownJobKindError struct {
	// Kind is the string that was returned by the JobArgs Kind method.
	Kind string
}

// Error returns the error string.
func (e *UnknownJobKindError) Error() string {
	return "job kind is not registered in the client's Workers bundle: " + e.Kind
}

// Is implements the interface used by errors.Is to determine if errors are
// equivalent. It returns true for any other UnknownJobKindError without
// regard to the Kind string so it is possible to detect this type of error
// with:
//
//	errors.Is(err, &UnknownJobKindError{})
func (e *UnknownJobKindError) Is(target error) bool {
	_, ok := target.(*UnknownJobKindError)
	return ok
}

// JobCancel wraps err and can be returned from a Worker's Work method to cancel
// the job at the end of execution. Regardless of whether or not the job has any
// remaining attempts, this will ensure the job does not execute again.
func JobCancel(err error) error {
	return &jobCancelError{err: err}
}

type jobCancelError struct {
	err error
}

func (e *jobCancelError) Error() string {
	// should not ever be called, but add a prefix just in case:
	return "jobCancelError: " + e.err.Error()
}

func (e *jobCancelError) Is(target error) bool {
	_, ok := target.(*jobCancelError)
	return ok
}

func (e *jobCancelError) Unwrap() error { return e.err }

// JobSnooze can be returned from a Worker's Work method to cause the job to be
// tried again after the specified duration. This also has the effect of
// incrementing the job's MaxAttempts by 1, meaning that jobs can be repeatedly
// snoozed without ever being discarded.
//
// Panics if duration is < 0.
func JobSnooze(duration time.Duration) error {
	if duration < 0 {
		panic("JobSnooze: duration must be >= 0")
	}
	return &jobSnoozeError{duration: duration}
}

type jobSnoozeError struct {
	duration time.Duration
}

func (e *jobSnoozeError) Error() string {
	// should not ever be called, but add a prefix just in case:
	return fmt.Sprintf("jobSnoozeError: %s", e.duration)
}

func (e *jobSnoozeError) Is(target error) bool {
	_, ok := target.(*jobSnoozeError)
	return ok
}

var ErrJobCancelledRemotely = JobCancel(errors.New("job cancelled remotely"))

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

type jobExecutor struct {
	baseservice.BaseService

	CancelFunc             context.CancelCauseFunc
	ClientJobTimeout       time.Duration
	Completer              jobcompleter.JobCompleter
	ClientRetryPolicy      ClientRetryPolicy
	ErrorHandler           ErrorHandler
	InformProducerDoneFunc func(jobRow *rivertype.JobRow)
	JobRow                 *rivertype.JobRow
	SchedulerInterval      time.Duration
	WorkUnit               workunit.WorkUnit

	// Meant to be used from within the job executor only.
	start time.Time
	stats *jobstats.JobStatistics // initialized by the executor, and handed off to completer
}

func (e *jobExecutor) Cancel() {
	e.Logger.Warn(e.Name+": job cancelled remotely", slog.Int64("job_id", e.JobRow.ID))
	e.CancelFunc(ErrJobCancelledRemotely)
}

func (e *jobExecutor) Execute(ctx context.Context) {
	// Ensure that the context is cancelled no matter what, or it will leak:
	defer e.CancelFunc(errExecutorDefaultCancel)

	e.start = e.Time.NowUTC()
	e.stats = &jobstats.JobStatistics{
		QueueWaitDuration: e.start.Sub(e.JobRow.ScheduledAt),
	}

	res := e.execute(ctx)
	if res.Err != nil && errors.Is(context.Cause(ctx), ErrJobCancelledRemotely) {
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
func (e *jobExecutor) execute(ctx context.Context) (res *jobExecutorResult) {
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
		return &jobExecutorResult{Err: &UnknownJobKindError{Kind: e.JobRow.Kind}}
	}

	if err := e.WorkUnit.UnmarshalJob(); err != nil {
		return &jobExecutorResult{Err: err}
	}

	{
		jobTimeout := e.WorkUnit.Timeout()
		if jobTimeout == 0 {
			jobTimeout = e.ClientJobTimeout
		}

		// No timeout if a -1 was specified.
		if jobTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, jobTimeout)
			defer cancel()
		}

		return &jobExecutorResult{Err: e.WorkUnit.Work(ctx)}
	}
}

func (e *jobExecutor) invokeErrorHandler(ctx context.Context, res *jobExecutorResult) bool {
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

func (e *jobExecutor) reportResult(ctx context.Context, res *jobExecutorResult) {
	var snoozeErr *jobSnoozeError

	if res.Err != nil && errors.As(res.Err, &snoozeErr) {
		e.Logger.InfoContext(ctx, e.Name+": Job snoozed",
			slog.Int64("job_id", e.JobRow.ID),
			slog.String("job_kind", e.JobRow.Kind),
			slog.Duration("duration", snoozeErr.duration),
		)
		nextAttemptScheduledAt := time.Now().Add(snoozeErr.duration)

		// Normally, snoozed jobs are set `scheduled` for the future and it's the
		// scheduler's job to set them back to `available` so they can be reworked.
		// Just as with retryable jobs, this isn't friendly for short snooze times
		// so we instead make the job immediately `available` if the snooze time is
		// smaller than the scheduler's run interval.
		var params *riverdriver.JobSetStateIfRunningParams
		if nextAttemptScheduledAt.Sub(e.Time.NowUTC()) <= e.SchedulerInterval {
			params = riverdriver.JobSetStateSnoozedAvailable(e.JobRow.ID, nextAttemptScheduledAt, e.JobRow.MaxAttempts+1)
		} else {
			params = riverdriver.JobSetStateSnoozed(e.JobRow.ID, nextAttemptScheduledAt, e.JobRow.MaxAttempts+1)
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

func (e *jobExecutor) reportError(ctx context.Context, res *jobExecutorResult) {
	var (
		cancelJob bool
		cancelErr *jobCancelError
	)

	logAttrs := []any{
		slog.String("error", res.ErrorStr()),
		slog.Int64("job_id", e.JobRow.ID),
		slog.String("job_kind", e.JobRow.Kind),
	}

	switch {
	case errors.As(res.Err, &cancelErr):
		cancelJob = true
		e.Logger.InfoContext(ctx, e.Name+": Job cancelled explicitly", logAttrs...)
	case res.Err != nil:
		e.Logger.ErrorContext(ctx, e.Name+": Job errored", logAttrs...)
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
			slog.Time("next_retry_scheduled_at", nextRetryScheduledAt),
			slog.Time("now", now),
		)
		nextRetryScheduledAt = (&DefaultClientRetryPolicy{}).NextRetry(e.JobRow)
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
