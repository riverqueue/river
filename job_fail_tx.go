package river

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// JobFailTx records jobErr as an attempt error and transitions the job to a
// failure state as part of transaction tx. If more attempts remain, the job
// is set to JobStateRetryable (or JobStateAvailable if the next retry is
// within the client's scheduler interval). If this was the final attempt
// (job.Attempt >= job.MaxAttempts), the job is set to JobStateDiscarded. If
// tx is rolled back, the state change will be as well.
//
// The returned job reflects the new state. Callers that keep external state
// in sync with job outcomes can inspect updatedJob.State - a state of
// JobStateDiscarded (or similar terminal state) indicates the job will not
// run again and any follow-up work (e.g. marking a web-app row "failed") can
// proceed inside the same transaction.
//
// The function needs to know the type of the River database driver, which is
// the same as the one in use by Client, but the other generic parameters can
// be inferred. An invocation should generally look like:
//
//	updatedJob, err := river.JobFailTx[*riverpgxv5.Driver](ctx, tx, job, jobErr)
//	if err != nil {
//		// handle error
//	}
//	if updatedJob.State == rivertype.JobStateDiscarded {
//		// job will not be retried; update external state accordingly
//	}
//
// Returns the updated, failed job.
func JobFailTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], jobErr error) (*Job[TArgs], error) {
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

	now := client.baseService.Time.Now()
	errData, err := marshalAttemptError(job.Attempt, now, jobErr)
	if err != nil {
		return nil, err
	}

	var params *riverdriver.JobSetStateIfRunningParams
	if job.Attempt >= job.MaxAttempts {
		params = riverdriver.JobSetStateDiscarded(job.ID, now, errData, metadataUpdatesBytes)
	} else {
		nextRetryFunc, ok := jobexecutor.NextRetryFromWorkContext(ctx)
		if !ok {
			return nil, errors.New("next retry function not found in context, can only work within a River worker")
		}
		scheduledAt, useAvailable := nextRetryFunc(ctx, now, job.JobRow)
		if useAvailable {
			params = riverdriver.JobSetStateErrorAvailable(job.ID, scheduledAt, errData, metadataUpdatesBytes)
		} else {
			params = riverdriver.JobSetStateErrorRetryable(job.ID, scheduledAt, errData, metadataUpdatesBytes)
		}
	}

	driver := client.Driver()
	pilot := client.Pilot()
	execTx := driver.UnwrapExecutor(tx)

	rows, err := pilot.JobSetStateIfRunningMany(ctx, execTx, setStateIfRunningManyParamsFromOne(client.config.Schema, params))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if _, isInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool); isInsideTestWorker {
			panic("to use JobFailTx in a rivertest.Worker, the job must be inserted into the database first")
		}

		return nil, rivertype.ErrNotFound
	}
	updatedJob := &Job[TArgs]{JobRow: rows[0]}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}
