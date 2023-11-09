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
	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

// UnknownJobKindError is returned when a Client fetches and attempts to
// work a job that has not been registered on the Client's Workers bundle (using
// AddWorker).
type UnknownJobKindError struct {
	// Kind is the string that was returned by the JobArgs Kind method.
	Kind string
}

// Error returns the error string.
func (e *UnknownJobKindError) Error() string {
	return fmt.Sprintf("job kind is not registered in the client's Workers bundle: %s", e.Kind)
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
	return fmt.Sprintf("jobCancelError: %s", e.err.Error())
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

type jobExecutorResult struct {
	Err        error
	NextRetry  time.Time
	PanicTrace []byte
	PanicVal   any
}

type jobExecutor struct {
	baseservice.BaseService

	Adapter                dbadapter.Adapter
	ClientJobTimeout       time.Duration
	Completer              jobcompleter.JobCompleter
	ClientRetryPolicy      ClientRetryPolicy
	ErrorHandler           ErrorHandler
	InformProducerDoneFunc func(jobRow *rivertype.JobRow)
	JobRow                 *rivertype.JobRow
	WorkUnit               workunit.WorkUnit

	// Meant to be used from within the job executor only.
	result *jobExecutorResult
	start  time.Time
	stats  *jobstats.JobStatistics // initialized by the executor, and handed off to completer
}

func (e *jobExecutor) Execute(ctx context.Context) {
	e.result = &jobExecutorResult{}
	e.start = e.TimeNowUTC()
	e.stats = &jobstats.JobStatistics{
		QueueWaitDuration: e.start.Sub(e.JobRow.ScheduledAt),
	}

	e.execute(ctx)

	e.reportResult(ctx)

	e.InformProducerDoneFunc(e.JobRow)
}

func (e *jobExecutor) execute(ctx context.Context) {
	defer func() {
		if recovery := recover(); recovery != nil {
			e.Logger.ErrorContext(ctx, e.Name+": jobExecutor panic recovery; possible bug with Worker",
				slog.Int64("job_id", e.JobRow.ID),
				slog.String("panic_val", fmt.Sprintf("%v", recovery)),
			)

			e.result.PanicVal = recovery
			e.result.PanicTrace = debug.Stack()
		}
		e.stats.RunDuration = e.TimeNowUTC().Sub(e.start)
	}()

	if e.WorkUnit == nil {
		e.Logger.ErrorContext(ctx, e.Name+": Unhandled job kind",
			slog.String("kind", e.JobRow.Kind),
			slog.Int64("job_id", e.JobRow.ID),
		)
		e.result.Err = &UnknownJobKindError{Kind: e.JobRow.Kind}
		return
	}

	if err := e.WorkUnit.UnmarshalJob(); err != nil {
		e.result.Err = err
		return
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

		e.result.Err = e.WorkUnit.Work(ctx)
	}
}

func (e *jobExecutor) reportResult(ctx context.Context) {
	var snoozeErr *jobSnoozeError

	if e.result.Err != nil && errors.As(e.result.Err, &snoozeErr) {
		e.Logger.InfoContext(ctx, e.Name+": Job snoozed",
			slog.Int64("job_id", e.JobRow.ID),
			slog.Duration("duration", snoozeErr.duration),
		)
		nextAttemptScheduledAt := time.Now().Add(snoozeErr.duration)
		if err := e.Completer.JobSetSnoozed(e.JobRow.ID, e.stats, nextAttemptScheduledAt); err != nil {
			e.Logger.ErrorContext(ctx, e.Name+": Error snoozing job",
				slog.Int64("job_id", e.JobRow.ID),
			)
		}
		return
	}

	if e.result.Err != nil || e.result.PanicVal != nil {
		e.reportError(ctx)
		return
	}

	if err := e.Completer.JobSetCompleted(e.JobRow.ID, e.stats, e.TimeNowUTC()); err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Error completing job",
			slog.String("err", err.Error()),
			slog.Int64("job_id", e.JobRow.ID),
		)
		return
	}
}

func (e *jobExecutor) reportError(ctx context.Context) {
	var (
		cancelJob    bool
		cancelErr    *jobCancelError
		errorHandler func() *ErrorHandlerResult
		errorStr     string
		fnName       string
	)

	switch {
	case errors.As(e.result.Err, &cancelErr):
		cancelJob = true
		e.Logger.InfoContext(ctx, e.Name+": Job cancelled explicitly",
			slog.String("err", cancelErr.err.Error()),
			slog.Int64("job_id", e.JobRow.ID),
		)
		errorStr = cancelErr.err.Error()

	case e.result.Err != nil:
		e.Logger.ErrorContext(ctx, e.Name+": Job failed",
			slog.String("err", e.result.Err.Error()),
			slog.Int64("job_id", e.JobRow.ID),
		)
		errorStr = e.result.Err.Error()

		if e.ErrorHandler != nil {
			fnName = "HandleError"
			errorHandler = func() *ErrorHandlerResult {
				return e.ErrorHandler.HandleError(ctx, e.JobRow, e.result.Err)
			}
		}

	case e.result.PanicVal != nil:
		e.Logger.ErrorContext(ctx, e.Name+": Job panicked", slog.Int64("job_id", e.JobRow.ID))
		errorStr = fmt.Sprintf("%v", e.result.PanicVal)

		if e.ErrorHandler != nil {
			fnName = "HandlePanic"
			errorHandler = func() *ErrorHandlerResult {
				return e.ErrorHandler.HandlePanic(ctx, e.JobRow, e.result.PanicVal)
			}
		}
	}

	var errorHandlerRes *ErrorHandlerResult
	if errorHandler != nil && !cancelJob {
		errorHandlerRes = func() *ErrorHandlerResult {
			defer func() {
				if panicVal := recover(); panicVal != nil {
					e.Logger.ErrorContext(ctx, e.Name+": ErrorHandler invocation panicked",
						slog.String("function_name", fnName),
						slog.String("panic_val", fmt.Sprintf("%v", panicVal)),
					)
				}
			}()

			return errorHandler()
		}()
		if errorHandlerRes != nil && errorHandlerRes.SetCancelled {
			cancelJob = true
		}
	}

	attemptErr := rivertype.AttemptError{
		At:    e.start,
		Error: errorStr,
		Num:   e.JobRow.Attempt,
		Trace: string(e.result.PanicTrace),
	}

	errData, err := json.Marshal(attemptErr)
	if err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Failed to marshal attempt error",
			slog.Int64("job_id", e.JobRow.ID),
			slog.String("err", err.Error()),
		)
		return
	}

	if cancelJob {
		if err := e.Completer.JobSetCancelled(e.JobRow.ID, e.stats, time.Now(), errData); err != nil {
			e.Logger.ErrorContext(ctx, e.Name+": Failed to cancel job and report error",
				slog.Int64("job_id", e.JobRow.ID),
			)
		}
		return
	}
	if e.JobRow.Attempt >= e.JobRow.MaxAttempts || (errorHandlerRes != nil && errorHandlerRes.SetCancelled) {
		if err := e.Completer.JobSetDiscarded(e.JobRow.ID, e.stats, time.Now(), errData); err != nil {
			e.Logger.ErrorContext(ctx, e.Name+": Failed to discard job and report error",
				slog.Int64("job_id", e.JobRow.ID),
			)
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
	if nextRetryScheduledAt.Before(time.Now()) {
		e.Logger.WarnContext(ctx,
			e.Name+": Retry policy returned invalid next retry before current time; using default retry policy instead",
			slog.Time("next_retry_scheduled_at", nextRetryScheduledAt),
		)
		nextRetryScheduledAt = (&DefaultClientRetryPolicy{}).NextRetry(e.JobRow)
	}

	if err := e.Completer.JobSetErrored(e.JobRow.ID, e.stats, nextRetryScheduledAt, errData); err != nil {
		e.Logger.ErrorContext(ctx, e.Name+": Failed to report error for job",
			slog.Int64("job_id", e.JobRow.ID),
		)
	}
}
