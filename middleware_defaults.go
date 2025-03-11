package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

type MiddlewareDefaults struct{}

func (d *MiddlewareDefaults) IsMiddleware() bool { return true }

// JobInsertMiddlewareDefaults is an embeddable struct that provides default
// implementations for the rivertype.JobInsertMiddleware. Use of this struct is
// recommended in case rivertype.JobInsertMiddleware is expanded in the future
// so that existing code isn't unexpectedly broken during an upgrade.
type JobInsertMiddlewareDefaults struct{ MiddlewareDefaults }

func (d *JobInsertMiddlewareDefaults) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

// WorkerInsertMiddlewareDefaults is an embeddable struct that provides default
// implementations for the rivertype.WorkerMiddleware. Use of this struct is
// recommended in case rivertype.WorkerMiddleware is expanded in the future so
// that existing code isn't unexpectedly broken during an upgrade.
type WorkerMiddlewareDefaults struct{ MiddlewareDefaults }

func (d *WorkerMiddlewareDefaults) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	return doInner(ctx)
}
