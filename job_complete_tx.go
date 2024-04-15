package river

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

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
	if job.State != rivertype.JobStateRunning {
		return nil, errors.New("job must be running")
	}

	var driver TDriver
	jobRow, err := driver.UnwrapExecutor(tx).JobSetStateIfRunning(ctx, riverdriver.JobSetStateCompleted(job.ID, time.Now()))
	if err != nil {
		return nil, err
	}

	updatedJob := &Job[TArgs]{JobRow: jobRow}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}
