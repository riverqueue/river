package river

// ErrorHandler provides an interface that will be invoked in case of an error
// or panic occurring in the job. This is often useful for logging and exception
// tracking, but can also be used to customize retry behavior.
type ErrorHandler interface {
	// HandleError is invoked in case of an error occurring in a job.
	HandleError(job *JobRow, err error) *ErrorHandlerResult

	// HandlePanic is invoked in case of a panic occurring in a job.
	HandlePanic(job *JobRow, panicVal any) *ErrorHandlerResult
}

type ErrorHandlerResult struct {
	// SetCancelled can be set to true to fail the job immediately and
	// permanently. By default it'll continue to follow the configured retry
	// schedule.
	SetCancelled bool
}
