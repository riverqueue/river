package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

var (
	_ rivertype.JobInsertMiddleware = &JobInsertMiddlewareDefaults{}
	_ rivertype.WorkerMiddleware    = &WorkerMiddlewareDefaults{}

	_ rivertype.JobInsertMiddleware = JobInsertMiddlewareFunc(func(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
		return doInner(ctx)
	})
	_ rivertype.Middleware = JobInsertMiddlewareFunc(func(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
		return doInner(ctx)
	})

	_ rivertype.Middleware = WorkerMiddlewareFunc(func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
		return doInner(ctx)
	})
	_ rivertype.WorkerMiddleware = WorkerMiddlewareFunc(func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
		return doInner(ctx)
	})
)
