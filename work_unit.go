package river

import (
	"context"
	"encoding/json"
	"time"
)

// workUnit provides an interface to a struct that wraps a job to be done
// combined with a work function that can execute it. Its main purpose is to
// wrap a struct that contains generic types (like a Worker[T] that needs to be
// invoked with a Job[T]) in such a way as to make it non-generic so that it can
// be used in other non-generic code like jobExecutor.
//
// Implemented by wrapperWorkUnit.
type workUnit interface {
	NextRetry() time.Time
	Timeout() time.Duration
	UnmarshalJob() error
	Work(ctx context.Context) error
}

// workUnitFactory provides an interface to a struct that can generate a
// workUnit, a wrapper around a job to be done combined with a work function
// that can execute it.
//
// Implemented by workUnitFactoryWrapper.
type workUnitFactory interface {
	// Make a workUnit, which wraps a job to be done and work function that can
	// execute it.
	MakeUnit(jobRow *JobRow) workUnit
}

// workUnitFactoryWrapper wraps a Worker to implement workUnitFactory.
type workUnitFactoryWrapper[T JobArgs] struct {
	worker Worker[T]
}

func (w *workUnitFactoryWrapper[T]) MakeUnit(jobRow *JobRow) workUnit {
	return &wrapperWorkUnit[T]{jobRow: jobRow, worker: w.worker}
}

// wrapperWorkUnit implements workUnit for a job and Worker.
type wrapperWorkUnit[T JobArgs] struct {
	job    *Job[T] // not set until after UnmarshalJob is invoked
	jobRow *JobRow
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
