package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

// MiddlewareDefaults should be embedded on any middleware implementation. It
// helps identify a struct as middleware, and guarantees forward compatibility in
// case additions are necessary to the rivertype.Middleware interface.
type MiddlewareDefaults struct{}

func (d *MiddlewareDefaults) IsMiddleware() bool { return true }

// JobInsertMiddlewareDefaults is an embeddable struct that provides default
// implementations for the rivertype.JobInsertMiddleware. Use of this struct is
// recommended in case rivertype.JobInsertMiddleware is expanded in the future
// so that existing code isn't unexpectedly broken during an upgrade.
//
// Deprecated: Prefer embedding the more general MiddlewareDefaults instead.
type JobInsertMiddlewareDefaults struct{ MiddlewareDefaults }

func (d *JobInsertMiddlewareDefaults) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

// WorkerInsertMiddlewareDefaults is an embeddable struct that provides default
// implementations for the rivertype.WorkerMiddleware. Use of this struct is
// recommended in case rivertype.WorkerMiddleware is expanded in the future so
// that existing code isn't unexpectedly broken during an upgrade.
//
// Deprecated: Prefer embedding the more general MiddlewareDefaults instead.
type WorkerMiddlewareDefaults struct{ MiddlewareDefaults }

func (d *WorkerMiddlewareDefaults) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	return doInner(ctx)
}
