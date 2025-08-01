package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

// HookDefaults should be embedded on any hooks implementation. It helps
// identify a struct as hooks, and guarantee forward compatibility in case
// additions are necessary to the rivertype.Hook interface.
type HookDefaults struct{}

func (d *HookDefaults) IsHook() bool { return true }

// HookInsertBeginFunc is a convenience helper for implementing
// rivertype.HookInsertBegin using a simple function instead of a struct.
type HookInsertBeginFunc func(ctx context.Context, params *rivertype.JobInsertParams) error

func (f HookInsertBeginFunc) InsertBegin(ctx context.Context, params *rivertype.JobInsertParams) error {
	return f(ctx, params)
}

func (f HookInsertBeginFunc) IsHook() bool { return true }

// HookWorkBeginFunc is a convenience helper for implementing
// rivertype.HookWorkBegin using a simple function instead of a struct.
type HookWorkBeginFunc func(ctx context.Context, job *rivertype.JobRow) error

func (f HookWorkBeginFunc) WorkBegin(ctx context.Context, job *rivertype.JobRow) error {
	return f(ctx, job)
}

func (f HookWorkBeginFunc) IsHook() bool { return true }

// HookWorkEndFunc is a convenience helper for implementing
// rivertype.HookWorkEnd using a simple function instead of a struct.
type HookWorkEndFunc func(ctx context.Context, job *rivertype.JobRow, err error) error

func (f HookWorkEndFunc) WorkEnd(ctx context.Context, job *rivertype.JobRow, err error) error {
	return f(ctx, job, err)
}

func (f HookWorkEndFunc) IsHook() bool { return true }
