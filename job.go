package river

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// Job represents a single unit of work, holding both the arguments and
// information for a job with args of type T.
type Job[T JobArgs] struct {
	*rivertype.JobRow

	// Args are the arguments for the job.
	Args T
}

// JobArgs is an interface that represents the arguments for a job of type T.
// These arguments are serialized into JSON and stored in the database.
type JobArgs interface {
	// Kind is a string that uniquely identifies the type of job. This must be
	// provided on your job arguments struct.
	Kind() string
}

// JobArgsWithInsertOpts is an extra interface that a job may implement on top
// of JobArgs to provide insertion-time options for all jobs of this type.
type JobArgsWithInsertOpts interface {
	// InsertOpts returns options for all jobs of this job type, overriding any
	// system defaults. These can also be overridden at insertion time.
	InsertOpts() InsertOpts
}

// JobCompleteTx marks the job as completed as part of transaction tx. If tx is
// rolled back, the completion will be as well.
//
// The function needs to know the type of the River database driver, which is
// the same as the one in use by Client, but the other generic parameters can be
// inferred. An invocation should generally look like:
//
//	_, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
//	if err != nil {
//		// handle error
//	}
//
// Returns the updated, completed job.
func JobCompleteTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs]) (*Job[TArgs], error) {
	if job.State != JobStateRunning {
		return nil, errors.New("job must be running")
	}

	var (
		driver  TDriver
		queries = &dbsqlc.Queries{}
	)

	internal, err := queries.JobSetState(ctx, driver.UnwrapTx(tx), dbsqlc.JobSetStateParams{
		ID:                  job.ID,
		FinalizedAtDoUpdate: true,
		FinalizedAt:         ptrutil.Ptr(time.Now()),
		State:               dbsqlc.JobStateCompleted,
	})
	if err != nil {
		return nil, err
	}

	updatedJob := &Job[TArgs]{JobRow: dbsqlc.JobRowFromInternal(internal)}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}

const (
	JobStateAvailable = rivertype.JobStateAvailable
	JobStateCancelled = rivertype.JobStateCancelled
	JobStateCompleted = rivertype.JobStateCompleted
	JobStateDiscarded = rivertype.JobStateDiscarded
	JobStateRetryable = rivertype.JobStateRetryable
	JobStateRunning   = rivertype.JobStateRunning
	JobStateScheduled = rivertype.JobStateScheduled
)

var jobStateAll = []rivertype.JobState{ //nolint:gochecknoglobals
	JobStateAvailable,
	JobStateCancelled,
	JobStateCompleted,
	JobStateDiscarded,
	JobStateRetryable,
	JobStateRunning,
	JobStateScheduled,
}
