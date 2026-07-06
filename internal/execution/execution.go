package execution

import (
	"context"
	"slices"

	"github.com/riverqueue/river/rivertype"
)

// ContextKeyInsideTestWorker is an internal context key that indicates whether
// the worker is running inside a [rivertest.Worker].
type ContextKeyInsideTestWorker struct{}

type Func func(ctx context.Context) error

// MiddlewareChain chains together the given middleware functions, returning a
// single function that applies them all in reverse order.
func MiddlewareChain(globalMiddleware []rivertype.Middleware, workerMiddleware []rivertype.WorkerMiddleware, doInner Func, jobRow *rivertype.JobRow) Func {
	// Quick return for no middleware, which will often be the case.
	if len(globalMiddleware) < 1 && len(workerMiddleware) < 1 {
		return doInner
	}

	// Wrap middlewares in reverse order so the one defined first is wrapped
	// as the outermost function and is first to receive the operation.
	for _, v := range slices.Backward(globalMiddleware) {
		middlewareItem := v.(rivertype.WorkerMiddleware) //nolint:forcetypeassert // capture the current middleware item
		previousDoInner := doInner                       // capture the current doInner function
		doInner = func(ctx context.Context) error {
			return middlewareItem.Work(ctx, jobRow, previousDoInner)
		}
	}

	for _, v := range slices.Backward(workerMiddleware) {
		middlewareItem := v        // capture the current middleware item
		previousDoInner := doInner // capture the current doInner function
		doInner = func(ctx context.Context) error {
			return middlewareItem.Work(ctx, jobRow, previousDoInner)
		}
	}

	return doInner
}
