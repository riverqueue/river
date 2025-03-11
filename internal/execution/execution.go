package execution

import (
	"context"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// ContextKeyInsideTestWorker is an internal context key that indicates whether
// the worker is running inside a [rivertest.Worker].
type ContextKeyInsideTestWorker struct{}

type Func func(ctx context.Context) error

// MaybeApplyTimeout returns a context that will be cancelled after the given
// timeout. If the timeout is <= 0, the context will not be timed out, but it
// will still have a cancel function returned. In either case the cancel
// function should be called after execution (in a defer).
func MaybeApplyTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	// No timeout if a -1 was specified.
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return context.WithCancel(ctx)
}

// MiddlewareChain chains together the given middleware functions, returning a
// single function that applies them all in reverse order.
func MiddlewareChain(globalMiddleware []rivertype.Middleware, workerMiddleware []rivertype.WorkerMiddleware, doInner Func, jobRow *rivertype.JobRow) Func {
	// Quick return for no middleware, which will often be the case.
	if len(globalMiddleware) < 1 && len(workerMiddleware) < 1 {
		return doInner
	}

	// Wrap middlewares in reverse order so the one defined first is wrapped
	// as the outermost function and is first to receive the operation.
	for i := len(globalMiddleware) - 1; i >= 0; i-- {
		middlewareItem := globalMiddleware[i].(rivertype.WorkerMiddleware) //nolint:forcetypeassert // capture the current middleware item
		previousDoInner := doInner                                         // capture the current doInner function
		doInner = func(ctx context.Context) error {
			return middlewareItem.Work(ctx, jobRow, previousDoInner)
		}
	}

	for i := len(workerMiddleware) - 1; i >= 0; i-- {
		middlewareItem := workerMiddleware[i] // capture the current middleware item
		previousDoInner := doInner            // capture the current doInner function
		doInner = func(ctx context.Context) error {
			return middlewareItem.Work(ctx, jobRow, previousDoInner)
		}
	}

	return doInner
}
