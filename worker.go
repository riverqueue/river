package river

import (
	"context"
	"fmt"
	"time"

	"github.com/riverqueue/river/internal/workunit"
)

// Worker is an interface that can perform a job with args of type T. A typical
// Worker implementation will be a struct that embeds WorkerDefaults, implements
// `Kind()` and `Work()`, and optionally overrides other methods to provide
// job-specific configuration for all jobs of that type:
//
//	type SleepArgs struct {
//		Duration time.Duration `json:"duration"`
//	}
//
//	func (SleepArgs) Kind() string { return "sleep" }
//
//	type SleepWorker struct {
//		WorkerDefaults[SleepArgs]
//	}
//
//	func (w *SleepWorker) Work(ctx context.Context, job *Job[SleepArgs]) error {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		case <-time.After(job.Args.Duration):
//			return nil
//		}
//	}
//
// In addition to fulfilling the Worker interface, workers must be registered
// with the client using the AddWorker function.
type Worker[T JobArgs] interface {
	// NextRetry calculates when the next retry for a failed job should take
	// place given when it was last attempted and its number of attempts, or any
	// other of the job's properties a user-configured retry policy might want
	// to consider.
	//
	// Note that this method on a worker overrides any client-level retry policy.
	// To use the client-level retry policy, return an empty `time.Time{}` or
	// include WorkerDefaults to do this for you.
	NextRetry(job *Job[T]) time.Time

	// Timeout is the maximum amount of time the job is allowed to run before
	// its context is cancelled. A timeout of zero (the default) means the job
	// will inherit the Client-level timeout. A timeout of -1 means the job's
	// context will never time out.
	Timeout(job *Job[T]) time.Duration

	// Work performs the job and returns an error if the job failed. The context
	// will be configured with a timeout according to the worker settings and may
	// be cancelled for other reasons.
	//
	// If no error is returned, the job is assumed to have succeeded and will be
	// marked completed.
	//
	// It is important for any worker to respect context cancellation to enable
	// the client to respond to shutdown requests; there is no way to cancel a
	// running job that does not respect context cancellation, other than
	// terminating the process.
	Work(ctx context.Context, job *Job[T]) error
}

// WorkerDefaults is an empty struct that can be embedded in your worker
// struct to make it fulfill the Worker interface with default values.
type WorkerDefaults[T JobArgs] struct{}

// NextRetry returns an empty time.Time{} to avoid setting any job or
// Worker-specific overrides on the next retry time. This means that the
// Client-level retry policy schedule will be used instead.
func (w WorkerDefaults[T]) NextRetry(*Job[T]) time.Time { return time.Time{} }

// Timeout returns the job-specific timeout. Override this method to set a
// job-specific timeout, otherwise the Client-level timeout will be applied.
func (w WorkerDefaults[T]) Timeout(*Job[T]) time.Duration { return 0 }

// AddWorker registers a Worker on the provided Workers bundle. Each Worker must
// be registered so that the Client knows it should handle a specific kind of
// job (as returned by its `Kind()` method).
//
// Use by explicitly specifying a JobArgs type and then passing an instance of a
// worker for the same type:
//
//	river.AddWorker(workers, &SortWorker{})
//
// Note that AddWorker can panic in some situations, such as if the worker is
// already registered or if its configuration is otherwise invalid. This default
// probably makes sense for most applications because you wouldn't want to start
// an application with invalid hardcoded runtime configuration. If you want to
// avoid panics, use AddWorkerSafely instead.
func AddWorker[T JobArgs](workers *Workers, worker Worker[T]) {
	if err := AddWorkerSafely[T](workers, worker); err != nil {
		panic(err)
	}
}

// AddWorkerSafely registers a worker on the provided Workers bundle. Unlike AddWorker,
// AddWorkerSafely does not panic and instead returns an error if the worker
// is already registered or if its configuration is invalid.
//
// Use by explicitly specifying a JobArgs type and then passing an instance of a
// worker for the same type:
//
//	river.AddWorkerSafely[SortArgs](workers, &SortWorker{}).
func AddWorkerSafely[T JobArgs](workers *Workers, worker Worker[T]) error {
	var jobArgs T
	return workers.add(jobArgs, &workUnitFactoryWrapper[T]{worker: worker})
}

// Workers is a list of available job workers. A Worker must be registered for
// each type of Job to be handled.
//
// Use the top-level AddWorker function combined with a Workers to register a
// worker.
type Workers struct {
	workersMap map[string]workerInfo // job kind -> worker info
}

// workerInfo bundles information about a registered worker for later lookup
// in a Workers bundle.
type workerInfo struct {
	jobArgs         JobArgs
	workUnitFactory workunit.WorkUnitFactory
}

// NewWorkers initializes a new registry of available job workers.
//
// Use the top-level AddWorker function combined with a Workers registry to
// register each available worker.
func NewWorkers() *Workers {
	return &Workers{
		workersMap: make(map[string]workerInfo),
	}
}

func (w Workers) add(jobArgs JobArgs, workUnitFactory workunit.WorkUnitFactory) error {
	kind := jobArgs.Kind()

	if _, ok := w.workersMap[kind]; ok {
		return fmt.Errorf("worker for kind %q is already registered", kind)
	}

	w.workersMap[kind] = workerInfo{
		jobArgs:         jobArgs,
		workUnitFactory: workUnitFactory,
	}

	return nil
}

// workFunc implements JobArgs and is used to wrap a function given to WorkFunc.
type workFunc[T JobArgs] struct {
	WorkerDefaults[T]
	kind string
	f    func(context.Context, *Job[T]) error
}

func (wf *workFunc[T]) Kind() string {
	return wf.kind
}

func (wf *workFunc[T]) Work(ctx context.Context, job *Job[T]) error {
	return wf.f(ctx, job)
}

// WorkFunc wraps a function to implement the Worker interface. A job args
// struct implementing JobArgs will still be required to specify a Kind.
//
// For example:
//
//	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[WorkFuncArgs]) error {
//		fmt.Printf("Message: %s", job.Args.Message)
//		return nil
//	}))
func WorkFunc[T JobArgs](f func(context.Context, *Job[T]) error) Worker[T] {
	return &workFunc[T]{f: f, kind: (*new(T)).Kind()}
}
