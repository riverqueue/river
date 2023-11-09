package workunit

import (
	"context"
	"time"

	"github.com/riverqueue/river/rivertype"
)

// WorkUnit provides an interface to a struct that wraps a job to be done
// combined with a work function that can execute it. Its main purpose is to
// wrap a struct that contains generic types (like a Worker[T] that needs to be
// invoked with a Job[T]) in such a way as to make it non-generic so that it can
// be used in other non-generic code like jobExecutor.
//
// Implemented by river.wrapperWorkUnit.
type WorkUnit interface {
	NextRetry() time.Time
	Timeout() time.Duration
	UnmarshalJob() error
	Work(ctx context.Context) error
}

// WorkUnitFactory provides an interface to a struct that can generate a
// workUnit, a wrapper around a job to be done combined with a work function
// that can execute it.
//
// Implemented by river.workUnitFactoryWrapper.
type WorkUnitFactory interface {
	// Make a workUnit, which wraps a job to be done and work function that can
	// execute it.
	MakeUnit(jobRow *rivertype.JobRow) WorkUnit
}
