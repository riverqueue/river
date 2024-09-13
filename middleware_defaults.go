package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

// JobMiddlewareDefaults is an embeddable struct that provides default
// implementations for the rivertype.JobMiddleware. Use of this struct is
// recommended in case rivertype.JobMiddleware is expanded in the future so that
// existing code isn't unexpectedly broken during an upgrade.
type JobMiddlewareDefaults struct{}

func (l *JobMiddlewareDefaults) Insert(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	return doInner(ctx)
}

func (l *JobMiddlewareDefaults) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	return doInner(ctx)
}

// func (l *JobLifecycleDefaults) InsertBegin(ctx context.Context, params *rivertype.JobLifecycleInsertParams) (context.Context, error) {
// 	return ctx, nil
// }

// func (l *JobLifecycleDefaults) InsertEnd(ctx context.Context, res *rivertype.JobInsertResult) error {
// 	return nil
// }

// func (l *JobLifecycleDefaults) InsertManyBegin(ctx context.Context, manyParams []*rivertype.JobLifecycleInsertParams) (context.Context, error) {
// 	return ctx, nil
// }

// func (l *JobLifecycleDefaults) InsertManyEnd(ctx context.Context, manyParams []*rivertype.JobLifecycleInsertParams) error {
// 	return nil
// }

// func (l *JobLifecycleDefaults) WorkBegin(ctx context.Context, job *rivertype.JobRow) (context.Context, error) {
// 	return ctx, nil
// }

// func (l *JobLifecycleDefaults) WorkEnd(ctx context.Context, job *rivertype.JobRow) error { return nil }
