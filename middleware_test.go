package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

type overridableJobMiddleware struct {
	JobInsertMiddlewareDefaults
	WorkerMiddlewareDefaults

	insertManyFunc func(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error)
	workFunc       func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error
}

func (m *overridableJobMiddleware) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	if m.insertManyFunc != nil {
		return m.insertManyFunc(ctx, manyParams, doInner)
	}
	return m.JobInsertMiddlewareDefaults.InsertMany(ctx, manyParams, doInner)
}

func (m *overridableJobMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	if m.workFunc != nil {
		return m.workFunc(ctx, job, doInner)
	}
	return m.WorkerMiddlewareDefaults.Work(ctx, job, doInner)
}
