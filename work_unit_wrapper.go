package river

import (
	"context"
	"encoding/json"
	"time"

	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

// workUnitFactoryWrapper wraps a Worker to implement workUnitFactory.
type workUnitFactoryWrapper[T JobArgs] struct {
	worker Worker[T]
}

func (w *workUnitFactoryWrapper[T]) MakeUnit(jobRow *rivertype.JobRow) workunit.WorkUnit {
	return &wrapperWorkUnit[T]{jobRow: jobRow, worker: w.worker}
}

// wrapperWorkUnit implements workUnit for a job and Worker.
type wrapperWorkUnit[T JobArgs] struct {
	job    *Job[T] // not set until after UnmarshalJob is invoked
	jobRow *rivertype.JobRow
	worker Worker[T]
}

func (w *wrapperWorkUnit[T]) NextRetry() time.Time           { return w.worker.NextRetry(w.job) }
func (w *wrapperWorkUnit[T]) Timeout() time.Duration         { return w.worker.Timeout(w.job) }
func (w *wrapperWorkUnit[T]) Work(ctx context.Context) error { return w.worker.Work(ctx, w.job) }

func (w *wrapperWorkUnit[T]) UnmarshalJob() error {
	w.job = &Job[T]{
		JobRow: w.jobRow,
	}

	return json.Unmarshal(w.jobRow.EncodedArgs, &w.job.Args)
}
