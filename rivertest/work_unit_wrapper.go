package rivertest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

// Everything in this file is duplicated from `./work_unit_wrapper.go`. I looked
// into deduplicating it, but unfortuntately it's quite difficult/impossible.
//
// We can't put it in `internal/` because it depends on top level `river` types
// so that'd immediately produce a cyclic dependency. The only approach that
// would work would be to export it from the top-level `river` which `rivertest`
// could then use, but then we'd be exporting some low-level plumbing for
// internal use, which isn't worth it.
//
// The original sin is that basic types like `Job`, `JobArgs`, `Worker`
// should've been in `rivertype` instead of the top-level `river`, then anything
// (including these types) could easily have been refactored to anywhere, but
// it's too late to do that now.
//
// The best thing to do for now is probably just to live with the duplication,
// so I've removed a todo that used to be here and replaced it with this
// explanatory comment.

// workUnitFactoryWrapper wraps a Worker to implement workUnitFactory.
type workUnitFactoryWrapper[T river.JobArgs] struct {
	worker river.Worker[T]
}

func (w *workUnitFactoryWrapper[T]) MakeUnit(jobRow *rivertype.JobRow) workunit.WorkUnit {
	return &wrapperWorkUnit[T]{jobRow: jobRow, worker: w.worker}
}

// wrapperWorkUnit implements workUnit for a job and Worker.
type wrapperWorkUnit[T river.JobArgs] struct {
	job    *river.Job[T] // not set until after UnmarshalJob is invoked
	jobRow *rivertype.JobRow
	worker river.Worker[T]
}

func (w *wrapperWorkUnit[T]) HookLookup(lookup *hooklookup.JobHookLookup) hooklookup.HookLookupInterface {
	var job T
	return lookup.ByJobArgs(job)
}

func (w *wrapperWorkUnit[T]) Middleware() []rivertype.WorkerMiddleware {
	return w.worker.Middleware(w.jobRow)
}
func (w *wrapperWorkUnit[T]) NextRetry() time.Time           { return w.worker.NextRetry(w.job) }
func (w *wrapperWorkUnit[T]) Timeout() time.Duration         { return w.worker.Timeout(w.job) }
func (w *wrapperWorkUnit[T]) Work(ctx context.Context) error { return w.worker.Work(ctx, w.job) }

func (w *wrapperWorkUnit[T]) UnmarshalJob() error {
	w.job = &river.Job[T]{
		JobRow: w.jobRow,
	}

	return json.Unmarshal(w.jobRow.EncodedArgs, &w.job.Args)
}
