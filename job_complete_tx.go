package river

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// JobCompleteTx marks the job as completed as part of transaction tx. If tx is
// rolled back, the completion will be as well.
//
// The function needs to know the type of the River database driver that is
// compatible with tx. This is usually the same driver used by Client, but may
// differ when a worker and its application transactions use different database
// abstractions. The other generic parameters can be inferred. An invocation
// should generally look like:
//
//	_, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
//	if err != nil {
//		// handle error
//	}
//
// After successfully committing the transaction, the worker should normally
// return nil. If it returns an error instead, the committed completion takes
// precedence. the job remains completed and a resulting subscribe event has
// kind EventKindJobCompleted. The error has no effect.
//
// Returns the updated, completed job.
func JobCompleteTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs]) (*Job[TArgs], error) {
	if job.State != rivertype.JobStateRunning {
		return nil, errors.New("job must be running")
	}

	clientData := clientContextDataFromContext(ctx)

	var driver TDriver

	// extract metadata updates from context
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	hasMetadataUpdates = hasMetadataUpdates && len(metadataUpdates) > 0
	var (
		metadataUpdatesBytes []byte
		marshalErr           error
	)
	if hasMetadataUpdates {
		metadataUpdatesBytes, marshalErr = json.Marshal(metadataUpdates)
		if marshalErr != nil {
			return nil, marshalErr
		}
	}

	execTx := driver.UnwrapExecutor(tx)
	params := riverdriver.JobSetStateCompleted(job.ID, clientData.Time, nil)
	rows, err := clientData.Pilot.JobSetStateIfRunningMany(ctx, execTx, &riverdriver.JobSetStateIfRunningManyParams{
		ID:              []int64{params.ID},
		Attempt:         []*int{params.Attempt},
		ErrData:         [][]byte{params.ErrData},
		FinalizedAt:     []*time.Time{params.FinalizedAt},
		MetadataDoMerge: []bool{hasMetadataUpdates},
		MetadataUpdates: [][]byte{metadataUpdatesBytes},
		ScheduledAt:     []*time.Time{params.ScheduledAt},
		Schema:          clientData.Schema,
		State:           []rivertype.JobState{params.State},
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if _, isInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool); isInsideTestWorker {
			panic("to use JobCompleteTx in a rivertest.Worker, the job must be inserted into the database first")
		}

		return nil, rivertype.ErrNotFound
	}
	updatedJob := &Job[TArgs]{JobRow: rows[0]}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}
