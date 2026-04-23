package river

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// ResumableSetStepTx immediately persists a resumable job's completed step as
// part of transaction tx. If tx is rolled back, the step update will be as
// well.
//
// Normally, a resumable job's step progress is recorded after it runs along
// with its result status. This is normally sufficient, but because it happens
// out-of-transaction, there's a chance that it doesn't happen in case of panic
// or other abrupt termination.  This function useful in cases where a resumable
// worker needs a guarantee of a checkpoint being recorded durably, at the cost
// of an extra database operation.
func ResumableSetStepTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], step string) (*Job[TArgs], error) {
	return resumableSetStepTx(ctx, tx, job, step, nil)
}

// ResumableSetStepCursorTx immediately persists a resumable job's completed
// step and cursor as part of transaction tx. If tx is rolled back, the step
// and cursor update will be as well.
//
// Normally, a resumable job's step progress is recorded after it runs along
// with its result status. This is normally sufficient, but because it happens
// out-of-transaction, there's a chance that it doesn't happen in case of panic
// or other abrupt termination.  This function useful in cases where a resumable
// worker needs a guarantee of a checkpoint being recorded durably, at the cost
// of an extra database operation.
func ResumableSetStepCursorTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs, TCursor any](ctx context.Context, tx TTx, job *Job[TArgs], step string, cursor TCursor) (*Job[TArgs], error) {
	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return nil, err
	}

	return resumableSetStepTx(ctx, tx, job, step, json.RawMessage(cursorBytes))
}

func resumableSetStepTx[TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], step string, cursor json.RawMessage) (*Job[TArgs], error) {
	if job.State != rivertype.JobStateRunning {
		return nil, errors.New("job must be running")
	}

	client := ClientFromContext[TTx](ctx)
	if client == nil {
		return nil, errors.New("client not found in context, can only work within a River worker")
	}

	metadataUpdates := map[string]any{
		rivercommon.MetadataKeyResumableStep: step,
	}

	if state, ok := resumableStateFromContext(ctx); ok {
		state.completedStep = step
		if cursor != nil {
			if state.cursors == nil {
				state.cursors = make(map[string]json.RawMessage)
			}
			state.cursors[step] = cursor
		}
		if len(state.cursors) > 0 {
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = state.cursors
		}
	} else if cursor != nil {
		metadataUpdates[rivercommon.MetadataKeyResumableCursor] = map[string]json.RawMessage{step: cursor}
	}

	workMetadataUpdates, hasWorkMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if hasWorkMetadataUpdates {
		workMetadataUpdates[rivercommon.MetadataKeyResumableStep] = step
		if resumableCursorMetadata, ok := metadataUpdates[rivercommon.MetadataKeyResumableCursor]; ok {
			workMetadataUpdates[rivercommon.MetadataKeyResumableCursor] = resumableCursorMetadata
		}
	}

	metadataUpdatesBytes, err := json.Marshal(metadataUpdates)
	if err != nil {
		return nil, err
	}

	updatedJob, err := client.Driver().UnwrapExecutor(tx).JobUpdate(ctx, &riverdriver.JobUpdateParams{
		ID:              job.ID,
		MetadataDoMerge: true,
		Metadata:        metadataUpdatesBytes,
		Schema:          client.config.Schema,
	})
	if err != nil {
		if errors.Is(err, rivertype.ErrNotFound) {
			if _, isInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool); isInsideTestWorker {
				panic("to use ResumableSetStepTx or ResumableSetStepCursorTx in a rivertest.Worker, the job must be inserted into the database first")
			}
		}

		return nil, err
	}

	result := &Job[TArgs]{JobRow: updatedJob}
	if err := json.Unmarshal(result.EncodedArgs, &result.Args); err != nil {
		return nil, err
	}

	return result, nil
}
