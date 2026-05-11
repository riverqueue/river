package rivermiddleware

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

// DefaultMiddleware returns the default middleware that River applies to all
// jobs. This includes internal middleware like the resumable step middleware.
func DefaultMiddleware() []rivertype.Middleware {
	return []rivertype.Middleware{&ResumableMiddleware{}}
}

// ResumableMiddleware is internal middleware that enables resumable step
// functionality. It reads the last completed step and cursor data from job
// metadata, injects them into the context, and persists updated step/cursor
// state back to metadata when a job errors after making progress.
type ResumableMiddleware struct{}

func (*ResumableMiddleware) IsMiddleware() bool { return true }

func (*ResumableMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	state := &ResumableState{
		Cursors:       make(map[string]json.RawMessage),
		ResumeMatched: true,
		ResumeStep:    gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableStep).Str,
	}
	if state.ResumeStep != "" {
		state.ResumeMatched = false
	}

	hadCursors := false
	if cursorJSON := gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableCursor); cursorJSON.Exists() && cursorJSON.Type == gjson.JSON {
		if err := json.Unmarshal([]byte(cursorJSON.Raw), &state.Cursors); err != nil {
			return fmt.Errorf("river: unmarshal resumable cursors: %w", err)
		}
		hadCursors = len(state.Cursors) > 0
	}

	ctx = context.WithValue(ctx, ResumableContextKey{}, state)

	err := doInner(ctx)
	if err == nil {
		switch {
		case state.Err != nil:
			err = state.Err
		case state.ResumeStep != "" && !state.ResumeMatched:
			err = fmt.Errorf("river: resumable step %q not found in Worker", state.ResumeStep)
		}
	}

	if err != nil && state.CompletedStep != "" {
		if len(state.Cursors) > 0 {
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = state.Cursors
		} else if hadCursors {
			// All cursors were consumed (their steps completed). Write null
			// to clear the stale cursor metadata, since the metadata merge
			// is additive and wouldn't remove the old key on its own.
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = nil
		}
		metadataUpdates[rivercommon.MetadataKeyResumableStep] = state.CompletedStep
	}

	return err
}

// ResumableState holds the state for a resumable job execution. It is stored in
// the context and accessed by ResumableStep and ResumableStepCursor.
type ResumableState struct {
	CompletedStep string
	Cursors       map[string]json.RawMessage
	Err           error
	ResumeMatched bool
	ResumeStep    string
	StepName      string
}

// ResumableContextKey is the context key for ResumableState.
type ResumableContextKey struct{}
