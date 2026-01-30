package rivertest

import (
	"context"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/middlewarelookup"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

func newTestJobExecutor(
	archetype *baseservice.Archetype,
	config *river.Config,
	completer jobcompleter.JobCompleter,
	job *rivertype.JobRow,
	workUnit workunit.WorkUnit,
	jobCancel context.CancelCauseFunc,
	executionDone chan struct{},
	resultErr *error,
) *jobexecutor.JobExecutor {
	return baseservice.Init(archetype, &jobexecutor.JobExecutor{
		CancelFunc:               jobCancel,
		ClientJobTimeout:         config.JobTimeout,
		ClientRetryPolicy:        config.RetryPolicy,
		Completer:                completer,
		DefaultClientRetryPolicy: &river.DefaultClientRetryPolicy{},
		ErrorHandler: &errorHandlerWrapper{
			HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *jobexecutor.ErrorHandlerResult {
				*resultErr = err
				return nil
			},
			HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *jobexecutor.ErrorHandlerResult {
				*resultErr = &PanicError{Cause: panicVal, Trace: trace}
				return nil
			},
		},
		HookLookupGlobal:       hooklookup.NewHookLookup(config.Hooks),
		HookLookupByJob:        hooklookup.NewJobHookLookup(),
		JobRow:                 job,
		MiddlewareLookupGlobal: middlewarelookup.NewMiddlewareLookup(config.Middleware),
		ProducerCallbacks: struct {
			JobDone func(jobRow *rivertype.JobRow)
			Stuck   func()
			Unstuck func()
		}{
			JobDone: func(job *rivertype.JobRow) { close(executionDone) },
			Stuck:   func() {},
			Unstuck: func() {},
		},
		SchedulerInterval: maintenance.JobSchedulerIntervalDefault,
		WorkUnit:          workUnit,
	})
}
