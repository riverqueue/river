package rivertest

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/valutil"
	"github.com/riverqueue/river/rivertype"
)

type Worker[T river.JobArgs, TTx any] struct {
	client *river.Client[TTx]
	config *river.Config
	worker river.Worker[T]
}

func NewWorker[T river.JobArgs, TTx any](t *testing.T, driver riverdriver.Driver[TTx], config *river.Config, worker river.Worker[T]) *Worker[T, TTx] {
	t.Helper()

	config = config.WithDefaults()
	client, err := river.NewClient(driver, config)
	require.NoError(t, err)

	return &Worker[T, TTx]{
		client: client,
		config: config,
		worker: worker,
	}
}

func (w *Worker[T, TTx]) Work(ctx context.Context, t *testing.T, args T) error {
	t.Helper()
	return w.WorkOpts(ctx, t, args, nil)
}

func (w *Worker[T, TTx]) WorkJob(ctx context.Context, t *testing.T, job *river.Job[T]) error {
	t.Helper()
	return w.WorkJobOpts(ctx, t, job, nil)
}

func (w *Worker[T, TTx]) WorkJobOpts(ctx context.Context, t *testing.T, job *river.Job[T], opts *river.InsertOpts) error {
	t.Helper()

	// populate river client into context:
	ctx = WorkContext(ctx, w.client)

	doInner := w.buildMiddlewareChain(job)
	return doInner(ctx)
}

func (w *Worker[T, TTx]) WorkOpts(ctx context.Context, t *testing.T, args T, opts *river.InsertOpts) error {
	t.Helper()

	job, err := makeJobFromArgs(t, w.config, args, opts)
	if err != nil {
		return err
	}

	return w.WorkJobOpts(ctx, t, job, opts)
}

func (w *Worker[T, TTx]) buildMiddlewareChain(job *river.Job[T]) func(ctx context.Context) error {
	// apply global middleware from client, as well as worker-specific middleware
	workerMiddleware := w.worker.Middleware(job)

	doInner := func(ctx context.Context) error {
		jobTimeout := w.worker.Timeout(job)
		if jobTimeout == 0 {
			jobTimeout = w.config.JobTimeout
		}

		// No timeout if a -1 was specified.
		// TODO: is this timeout being applied in the wrong place? Should it apply
		// on the *outside* of all middleware?
		if jobTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, jobTimeout)
			defer cancel()
		}

		return w.worker.Work(ctx, job)
	}

	allMiddleware := make([]rivertype.WorkerMiddleware, 0, len(w.config.WorkerMiddleware)+len(workerMiddleware))
	allMiddleware = append(allMiddleware, w.config.WorkerMiddleware...)
	allMiddleware = append(allMiddleware, workerMiddleware...)

	if len(allMiddleware) > 0 {
		// Wrap middlewares in reverse order so the one defined first is wrapped
		// as the outermost function and is first to receive the operation.
		for i := len(allMiddleware) - 1; i >= 0; i-- {
			middlewareItem := allMiddleware[i] // capture the current middleware item
			previousDoInner := doInner         // Capture the current doInner function
			doInner = func(ctx context.Context) error {
				return middlewareItem.Work(ctx, job.JobRow, previousDoInner)
			}
		}
	}

	return doInner
}

var idSeq int64 = 0 //nolint:gochecknoglobals

func nextID() int64 {
	return atomic.AddInt64(&idSeq, 1)
}

func makeJobFromArgs[T river.JobArgs](t *testing.T, config *river.Config, args T, opts *river.InsertOpts) (*river.Job[T], error) {
	t.Helper()

	if opts == nil {
		opts = &river.InsertOpts{}
	}

	created := time.Now().Add(-1500 * time.Millisecond)

	encodedArgs, err := json.Marshal(args)
	require.NoError(t, err)

	// Round trip the args to validate JSON marshalling/unmarshalling on the type.
	var decodedArgs T
	if err = json.Unmarshal(encodedArgs, &decodedArgs); err != nil {
		return nil, err
	}

	// TODO: use insertParamsFromConfigArgsAndOptions instead of reimplementing it here?

	var jobInsertOpts river.InsertOpts
	if argsWithOpts, ok := any(args).(river.JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}

	maxAttempts := valutil.FirstNonZero(opts.MaxAttempts, jobInsertOpts.MaxAttempts, config.MaxAttempts)

	return &river.Job[T]{
		Args: decodedArgs,
		JobRow: &rivertype.JobRow{
			ID:          nextID(),
			Attempt:     1,
			AttemptedAt: ptrutil.Ptr(time.Now()),
			AttemptedBy: []string{"worker1"},
			CreatedAt:   created,
			EncodedArgs: encodedArgs,
			Errors:      []rivertype.AttemptError{},
			FinalizedAt: nil,
			Kind:        args.Kind(),
			MaxAttempts: maxAttempts,
			Metadata:    []byte(`{}`),
			Priority:    1,
			Queue:       "default",
			ScheduledAt: created,
			State:       rivertype.JobStateRunning,
			Tags:        []string{},
			UniqueKey:   []byte{},
		},
	}, nil
}
