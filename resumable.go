package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivertype"
)

var (
	errResumableStepNotInWorker = errors.New("river: resumable step can only be used within a Worker")
	errResumableCursorNotInStep = errors.New("river: resumable cursor can only be used within ResumableStepCursor")
)

type resumableContextKey struct{}

type resumableState struct {
	completedStep string
	cursors       map[string]json.RawMessage
	err           error
	resumeMatched bool
	resumeStep    string
	stepName      string
}

// ResumableSetCursor records a cursor for the current resumable cursor step.
// The cursor is stored only if the job attempt ends in an error, allowing a
// later retry to resume the same step from the recorded position.
//
// Alternatively, ResumableSetStepCursorTx is available to persist a step and
// cursor immediately as part of a transaction, guaranteeing that it's stored
// durably.
func ResumableSetCursor[TCursor any](ctx context.Context, cursor TCursor) error {
	state := mustResumableState(ctx)
	if state.stepName == "" {
		return errResumableCursorNotInStep
	}

	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return err
	}

	if state.cursors == nil {
		state.cursors = make(map[string]json.RawMessage)
	}
	state.cursors[state.stepName] = json.RawMessage(cursorBytes)
	return nil
}

// ResumableStep runs a resumable step, skipping the step on a later retry if
// an earlier attempt already completed it successfully.
//
// After a step returns an error, no subsequent steps will be run and the
// overall job will be marked as failed with that error. Be careful to put all
// executable code in steps, because any code outside of them will be run, even
// if a step returned an error.
func ResumableStep(ctx context.Context, name string, stepFunc func(ctx context.Context) error) {
	state := mustResumableState(ctx)
	if state.err != nil {
		return
	}

	if !state.resumeMatched {
		if name == state.resumeStep {
			state.completedStep = name
			state.resumeMatched = true
		}
		return
	}

	if err := stepFunc(ctx); err != nil {
		state.err = err
		return
	}

	state.completedStep = name
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
func ResumableStepCursor[TCursor any](ctx context.Context, name string, stepFunc func(ctx context.Context, cursor TCursor) error) {
	state := mustResumableState(ctx)
	if state.err != nil {
		return
	}

	if !state.resumeMatched {
		if name == state.resumeStep {
			state.completedStep = name
			state.resumeMatched = true
		}
		return
	}

	var cursor TCursor
	if cursorBytes, ok := state.cursors[name]; ok && len(cursorBytes) > 0 {
		if err := json.Unmarshal(cursorBytes, &cursor); err != nil {
			state.err = fmt.Errorf("river: unmarshal resumable cursor for step %q: %w", name, err)
			return
		}
	}

	previousStepName := state.stepName
	state.stepName = name
	defer func() { state.stepName = previousStepName }()

	if err := stepFunc(ctx, cursor); err != nil {
		state.err = err
		return
	}

	state.completedStep = name
	delete(state.cursors, name)
}

type resumableMiddleware struct {
	MiddlewareDefaults
}

func (*resumableMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	state := &resumableState{
		cursors:       make(map[string]json.RawMessage),
		resumeMatched: true,
		resumeStep:    gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableStep).Str,
	}
	if state.resumeStep != "" {
		state.resumeMatched = false
	}
	if cursorJSON := gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableCursor); cursorJSON.Exists() && cursorJSON.Type == gjson.JSON {
		if err := json.Unmarshal([]byte(cursorJSON.Raw), &state.cursors); err != nil {
			return fmt.Errorf("river: unmarshal resumable cursors: %w", err)
		}
	}

	ctx = context.WithValue(ctx, resumableContextKey{}, state)

	err := doInner(ctx)
	if err == nil {
		switch {
		case state.err != nil:
			err = state.err
		case state.resumeStep != "" && !state.resumeMatched:
			err = fmt.Errorf("river: resumable step %q not found in Worker", state.resumeStep)
		}
	}

	if err != nil && state.completedStep != "" {
		if len(state.cursors) > 0 {
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = state.cursors
		}
		metadataUpdates[rivercommon.MetadataKeyResumableStep] = state.completedStep
	}

	return err
}

func mustResumableState(ctx context.Context) *resumableState {
	typedState, ok := resumableStateFromContext(ctx)
	if !ok {
		panic(errResumableStepNotInWorker)
	}

	return typedState
}

func resumableStateFromContext(ctx context.Context) (*resumableState, bool) {
	state := ctx.Value(resumableContextKey{})
	if state == nil {
		return nil, false
	}

	typedState, ok := state.(*resumableState)
	if !ok || typedState == nil {
		return nil, false
	}

	return typedState, true
}
