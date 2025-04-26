package example_global_hooks

import (
	"context"
	"fmt"

	"github.com/riverqueue/river"
)

//
// This file used as a holding place for test helpers for examples so that the
// helpers aren't included in Godoc and keep each example more succinct.
//

type NoOpArgs struct{}

func (NoOpArgs) Kind() string { return "no_op" }

type NoOpWorker struct {
	river.WorkerDefaults[NoOpArgs]
}

func (w *NoOpWorker) Work(ctx context.Context, job *river.Job[NoOpArgs]) error {
	fmt.Printf("NoOpWorker.Work ran\n")
	return nil
}
