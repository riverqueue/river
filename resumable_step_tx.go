package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// ResumableSetStepTx immediately persists the current resumable step as
// part of transaction tx. If tx is rolled back, the step update will be as
// well. Earlier progress not already persisted may be replayed after rollback.
//
// Normally, a resumable job's step progress is recorded after it runs along
// with its result status. This is normally sufficient, but because it happens
// out-of-transaction, there's a chance that it doesn't happen in case of panic
// or other abrupt termination. This function is useful in cases where a
// resumable worker needs a guarantee of a checkpoint being recorded durably, at
// the cost of an extra database operation.
//
// Must be called from within a ResumableStep or ResumableStepCursor callback.
// The current step name to persist is read from context.
// On a cursor-only retry, checkpointing another step returns an error until
// the saved cursor step is reached.
//
// Commit tx before the step callback returns successfully, and return any
// transaction or commit error from that callback. A successful callback marks
// the step complete even if an enclosing transaction later rolls back.
func ResumableSetStepTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs]) (*Job[TArgs], error) {
	return resumableSetStepTx(ctx, tx, job, nil)
}

// ResumableSetStepCursorTx immediately persists the current resumable step and
// cursor as part of transaction tx. If tx is rolled back, the step and cursor
// update will be as well. Earlier progress not already persisted may be
// replayed after rollback.
//
// Normally, a resumable job's step progress is recorded after it runs along
// with its result status. This is normally sufficient, but because it happens
// out-of-transaction, there's a chance that it doesn't happen in case of panic
// or other abrupt termination. This function is useful in cases where a
// resumable worker needs a guarantee of a checkpoint being recorded durably, at
// the cost of an extra database operation.
//
// Must be called from within a ResumableStepCursor callback. The current step
// name to persist is read from context.
// On a cursor-only retry, checkpointing another step returns an error until
// the saved cursor step is reached.
//
// Commit tx before the step callback returns successfully, and return any
// transaction or commit error from that callback. A successful callback marks
// the step complete even if an enclosing transaction later rolls back.
func ResumableSetStepCursorTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs, TCursor any](ctx context.Context, tx TTx, job *Job[TArgs], cursor TCursor) (*Job[TArgs], error) {
	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return nil, err
	}

	return resumableSetStepTx(ctx, tx, job, json.RawMessage(cursorBytes))
}

func resumableSetStepTx[TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], cursor json.RawMessage) (*Job[TArgs], error) {
	if job.State != rivertype.JobStateRunning {
		return nil, errors.New("job must be running")
	}

	state, ok := resumableStateFromContext(ctx)
	if !ok {
		return nil, errors.New("not inside a resumable step; must be called from within ResumableStep or ResumableStepCursor")
	}
	if state.StepName == "" {
		return nil, errors.New("not inside a resumable step; must be called from within ResumableStep or ResumableStepCursor")
	}

	if len(state.PendingCursorSteps) > 0 {
		return nil, fmt.Errorf("river: resumable checkpoint cannot precede saved cursor step %q", state.PendingCursorSteps[0])
	}

	step := state.StepName

	client := ClientFromContext[TTx](ctx)
	if client == nil {
		return nil, errors.New("client not found in context, can only work within a River worker")
	}

	metadataUpdates := map[string]any{
		rivercommon.MetadataKeyResumableStep: step,
	}

	cursors := maps.Clone(state.Cursors)
	if cursors == nil {
		cursors = make(map[string]json.RawMessage)
	}
	if cursor != nil {
		cursors[step] = cursor
	}
	metadataUpdates[rivercommon.MetadataKeyResumableCursor] = nil
	if len(cursors) > 0 {
		metadataUpdates[rivercommon.MetadataKeyResumableCursor] = cursors
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

	// The database owns this checkpoint, including rollback of the caller's
	// transaction. Only progress recorded after this write belongs in deferred
	// attempt metadata; replaying it could persist rolled-back application work.
	state.CompletedStep = ""
	state.Cursors = cursors
	state.CursorsDirty = false

	result := &Job[TArgs]{JobRow: updatedJob}
	if err := json.Unmarshal(result.EncodedArgs, &result.Args); err != nil {
		return nil, err
	}

	return result, nil
}
