package execution

import (
	"context"
	"time"

	"github.com/riverqueue/river/rivertype"
)

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
	allMiddleware := make([]rivertype.WorkerMiddleware, 0, len(global)+len(worker))
	allMiddleware = append(allMiddleware, global...)
	allMiddleware = append(allMiddleware, worker...)

	if len(allMiddleware) > 0 {
		// Wrap middlewares in reverse order so the one defined first is wrapped
		// as the outermost function and is first to receive the operation.
		for i := len(allMiddleware) - 1; i >= 0; i-- {
			middlewareItem := allMiddleware[i] // capture the current middleware item
			previousDoInner := doInner         // capture the current doInner function
			doInner = func(ctx context.Context) error {
				return middlewareItem.Work(ctx, jobRow, previousDoInner)
			}
		}
	}

	return doInner
}
