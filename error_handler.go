package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

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
