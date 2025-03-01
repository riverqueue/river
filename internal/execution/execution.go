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
func MiddlewareChain(global, worker []rivertype.WorkerMiddleware, doInner Func, jobRow *rivertype.JobRow) Func {
	// Quick return for no middleware, which will often be the case.
	if len(global) < 1 && len(worker) < 1 {
		return doInner
	}

	// Write this so as to avoid a new slice allocation in cases where there is
	// no worker specific middleware (which will be the common case).
	allMiddleware := global
	if len(worker) > 0 {
		allMiddleware = append(allMiddleware, worker...)
	}

	// Wrap middlewares in reverse order so the one defined first is wrapped
	// as the outermost function and is first to receive the operation.
	for i := len(allMiddleware) - 1; i >= 0; i-- {
		middlewareItem := allMiddleware[i] // capture the current middleware item
		previousDoInner := doInner         // capture the current doInner function
		doInner = func(ctx context.Context) error {
			return middlewareItem.Work(ctx, jobRow, previousDoInner)
		}
	}

	return doInner
}
