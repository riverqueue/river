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

	// extract metadata updates from context
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	hasMetadataUpdates = hasMetadataUpdates && len(metadataUpdates) > 0
	var (
		metadataUpdatesBytes []byte
		err                  error
	)
	if hasMetadataUpdates {
		metadataUpdatesBytes, err = json.Marshal(metadataUpdates)
		if err != nil {
			return nil, err
		}
	}

	execTx := driver.UnwrapExecutor(tx)
	params := riverdriver.JobSetStateCompleted(job.ID, client.baseService.Time.NowUTC(), nil)
	rows, err := pilot.JobSetStateIfRunningMany(ctx, execTx, &riverdriver.JobSetStateIfRunningManyParams{
		ID:              []int64{params.ID},
		Attempt:         []*int{params.Attempt},
		ErrData:         [][]byte{params.ErrData},
		FinalizedAt:     []*time.Time{params.FinalizedAt},
		MetadataDoMerge: []bool{hasMetadataUpdates},
		MetadataUpdates: [][]byte{metadataUpdatesBytes},
		ScheduledAt:     []*time.Time{params.ScheduledAt},
		Schema:          client.config.Schema,
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
