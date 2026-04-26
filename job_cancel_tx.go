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

// JobCancelTx marks the job as cancelled as part of transaction tx, appending
// jobErr as an AttemptError on the job. If tx is rolled back, the cancellation
// will be as well. The job will not be retried regardless of how many attempts
// remain, mirroring the behavior of returning JobCancel(jobErr) from a
// Worker's Work method but keeping the state change in sync with other
// database writes in tx.
//
// The function needs to know the type of the River database driver, which is
// the same as the one in use by Client, but the other generic parameters can be
// inferred. An invocation should generally look like:
//
//	_, err := river.JobCancelTx[*riverpgxv5.Driver](ctx, tx, job, jobErr)
//	if err != nil {
//		// handle error
//	}
//
// Returns the updated, cancelled job.
func JobCancelTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], jobErr error) (*Job[TArgs], error) {
	if job.State != rivertype.JobStateRunning {
		return nil, errors.New("job must be running")
	}

	client := ClientFromContext[TTx](ctx)
	if client == nil {
		return nil, errors.New("client not found in context, can only work within a River worker")
	}

	metadataUpdatesBytes, err := marshalMetadataUpdatesFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap jobErr in JobCancel so the recorded AttemptError.Error matches
	// what the executor persists for a worker returning river.JobCancel(err).
	// If the caller already passed a JobCancelError (e.g. via river.JobCancel
	// themselves), keep it as-is to avoid double-wrapping.
	if !errors.Is(jobErr, &rivertype.JobCancelError{}) {
		jobErr = rivertype.JobCancel(jobErr)
	}

	now := client.baseService.Time.Now()
	errData, err := marshalAttemptError(job.Attempt, now, jobErr)
	if err != nil {
		return nil, err
	}

	driver := client.Driver()
	pilot := client.Pilot()
	execTx := driver.UnwrapExecutor(tx)

	params := riverdriver.JobSetStateCancelled(job.ID, now, errData, metadataUpdatesBytes)
	rows, err := pilot.JobSetStateIfRunningMany(ctx, execTx, setStateIfRunningManyParamsFromOne(client.config.Schema, params))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if _, isInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool); isInsideTestWorker {
			panic("to use JobCancelTx in a rivertest.Worker, the job must be inserted into the database first")
		}

		return nil, rivertype.ErrNotFound
	}
	updatedJob := &Job[TArgs]{JobRow: rows[0]}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}

// marshalMetadataUpdatesFromContext extracts metadata updates from the
// jobexecutor work context and JSON-marshals them for a JobSetState* call.
// Returns nil bytes when there are no updates. This is shared logic for
// JobCompleteTx, JobFailTx, and JobCancelTx.
func marshalMetadataUpdatesFromContext(ctx context.Context) ([]byte, error) {
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates || len(metadataUpdates) == 0 {
		return nil, nil
	}
	return json.Marshal(metadataUpdates)
}

// marshalAttemptError builds and JSON-marshals a rivertype.AttemptError from
// jobErr. A nil jobErr is allowed and produces AttemptError.Error == "<nil>".
func marshalAttemptError(attempt int, now time.Time, jobErr error) ([]byte, error) {
	errStr := "<nil>"
	if jobErr != nil {
		errStr = jobErr.Error()
	}
	return json.Marshal(rivertype.AttemptError{
		At:      now,
		Attempt: attempt,
		Error:   errStr,
	})
}

// setStateIfRunningManyParamsFromOne wraps a single JobSetStateIfRunningParams
// into the "many" variant used by pilot.JobSetStateIfRunningMany. Shared by
// JobCompleteTx, JobFailTx, and JobCancelTx.
func setStateIfRunningManyParamsFromOne(schema string, params *riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
	return &riverdriver.JobSetStateIfRunningManyParams{
		ID:              []int64{params.ID},
		Attempt:         []*int{params.Attempt},
		ErrData:         [][]byte{params.ErrData},
		FinalizedAt:     []*time.Time{params.FinalizedAt},
		MetadataDoMerge: []bool{params.MetadataDoMerge},
		MetadataUpdates: [][]byte{params.MetadataUpdates},
		ScheduledAt:     []*time.Time{params.ScheduledAt},
		Schema:          schema,
		State:           []rivertype.JobState{params.State},
	}
}
