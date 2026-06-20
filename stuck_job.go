package river

import "context"

// JobStuckHandler is invoked when a producer detects that a job exceeded its
// timeout and did not return within the configured stuck-job timeout margin.
type JobStuckHandler func(ctx context.Context, params JobStuckHandlerParams) JobStuckHandlerResult

// JobStuckHandlerParams are parameters passed to JobStuckHandler.
type JobStuckHandlerParams struct {
	// ID is the ID of the stuck job.
	ID int64

	// Kind is the kind of the stuck job.
	Kind string

	// Queue is the queue where the stuck job is running.
	Queue string

	// TotalStuckJobs is the total number of jobs currently considered stuck
	// across the client (includes all queues).
	TotalStuckJobs int
}

// JobStuckHandlerResult is the result returned by JobStuckHandler.
type JobStuckHandlerResult struct {
	// AddWorkerSlot instructs River to treat the stuck job as no longer
	// occupying a worker slot so another job can begin executing. This can be
	// dangerous because the stuck job's goroutine is still running, so the
	// queue may temporarily have more active job goroutines than MaxWorkers.
	AddWorkerSlot bool
}
