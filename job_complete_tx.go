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

	client := ClientFromContext[TTx](ctx)
	if client == nil {
		return nil, errors.New("client not found in context, can only work within a River worker")
	}

	driver := client.Driver()
	pilot := client.Pilot()

	execTx := driver.UnwrapExecutor(tx)
	params := riverdriver.JobSetStateCompleted(job.ID, time.Now())
	rows, err := pilot.JobSetStateIfRunningMany(ctx, execTx, &riverdriver.JobSetStateIfRunningManyParams{
		ID:          []int64{params.ID},
		ErrData:     [][]byte{params.ErrData},
		FinalizedAt: []*time.Time{params.FinalizedAt},
		MaxAttempts: []*int{params.MaxAttempts},
		ScheduledAt: []*time.Time{params.ScheduledAt},
		State:       []rivertype.JobState{params.State},
	})
	if err != nil {
		return nil, err
	}
	updatedJob := &Job[TArgs]{JobRow: rows[0]}

	if err := json.Unmarshal(updatedJob.EncodedArgs, &updatedJob.Args); err != nil {
		return nil, err
	}

	return updatedJob, nil
}
