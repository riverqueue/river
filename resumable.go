package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/riverqueue/river/internal/rivermiddleware"
)

var (
	errResumableStepNotInWorker = errors.New("river: resumable step can only be used within a Worker")
	errResumableCursorNotInStep = errors.New("river: resumable cursor can only be used within ResumableStepCursor")
)

// ResumableSetCursor records a cursor for the current resumable cursor step.
// The cursor is stored only if the job attempt ends in an error, allowing a
// later retry to resume the same step from the recorded position.
//
// Alternatively, ResumableSetStepCursorTx is available to persist a step and
// cursor immediately as part of a transaction, guaranteeing that it's stored
// durably.
func ResumableSetCursor[TCursor any](ctx context.Context, cursor TCursor) error {
	state := mustResumableState(ctx)
	if state.StepName == "" {
		return errResumableCursorNotInStep
	}

	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return err
	}

	if state.Cursors == nil {
		state.Cursors = make(map[string]json.RawMessage)
	}
	state.Cursors[state.StepName] = json.RawMessage(cursorBytes)
	return nil
}

// StepOpts are options for ResumableStep and ResumableStepCursor. There are
// currently no available options, but this space is reserved for future use.
type StepOpts struct{}

// ResumableStep runs a resumable step, skipping the step on a later retry if
// an earlier attempt already completed it successfully.
//
// After a step returns an error, no subsequent steps will be run and the
// overall job will be marked as failed with that error. Be careful to put all
// executable code in steps, because any code outside of them will be run, even
// if a step returned an error.
//
// opts may be nil.
func ResumableStep(ctx context.Context, name string, opts *StepOpts, stepFunc func(ctx context.Context) error) {
	state := mustResumableState(ctx)
	if state.Err != nil {
		return
	}

	if !state.ResumeMatched {
		if name == state.ResumeStep {
			state.CompletedStep = name
			state.ResumeMatched = true
		}
		return
	}

	previousStepName := state.StepName
	state.StepName = name
	defer func() { state.StepName = previousStepName }()

	if err := stepFunc(ctx); err != nil {
		state.Err = err
		return
	}

	state.CompletedStep = name
}

// ResumableStepCursor runs a resumable step that also receives a persisted
// cursor value from an earlier failed attempt, if one was recorded with
// ResumableSetCursor.
//
// The cursor type T is user-specified. It may be a primitive value like an
// integer ID, or a more complex type like a struct with multiple fields. It's
// stored in a job's metadata, so it needs to be marshable and unmarshable to
// and from JSON.
//
// Notably, it's the responsibility of the step function to call
// ResumableSetCursor with an updated cursor value as progress is made, and to
// check the cursor value before running to determine where to resume from.
//
// After a step returns an error, no subsequent steps will be run and the
// overall job will be marked as failed with that error. Be careful to put all
// executable code in steps, because any code outside of them will be run, even
// if a step returned an error.
//
// opts may be nil.
func ResumableStepCursor[TCursor any](ctx context.Context, name string, opts *StepOpts, stepFunc func(ctx context.Context, cursor TCursor) error) {
	state := mustResumableState(ctx)
	if state.Err != nil {
		return
	}

	if !state.ResumeMatched {
		if name == state.ResumeStep {
			state.CompletedStep = name
			state.ResumeMatched = true

			// If cursor data exists for this step, it was only partially
			// completed on the previous attempt. Fall through to re-execute
			// it with the cursor rather than skipping it.
			if _, hasCursor := state.Cursors[name]; !hasCursor {
				return
			}
		} else {
			return
		}
	}

	var cursor TCursor
	if cursorBytes, ok := state.Cursors[name]; ok && len(cursorBytes) > 0 {
		if err := json.Unmarshal(cursorBytes, &cursor); err != nil {
			state.Err = fmt.Errorf("river: unmarshal resumable cursor for step %q: %w", name, err)
			return
		}
	}

	previousStepName := state.StepName
	state.StepName = name
	defer func() { state.StepName = previousStepName }()

	if err := stepFunc(ctx, cursor); err != nil {
		state.Err = err
		return
	}

	state.CompletedStep = name
	delete(state.Cursors, name)
}

func mustResumableState(ctx context.Context) *rivermiddleware.ResumableState {
	state, ok := resumableStateFromContext(ctx)
	if !ok {
		panic(errResumableStepNotInWorker)
	}

	return state
}

func resumableStateFromContext(ctx context.Context) (*rivermiddleware.ResumableState, bool) {
	state := ctx.Value(rivermiddleware.ResumableContextKey{})
	if state == nil {
		return nil, false
	}

	typedState, ok := state.(*rivermiddleware.ResumableState)
	if !ok || typedState == nil {
		return nil, false
	}

	return typedState, true
}
