package riverplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivertype"
)

// DefaultPlugins returns the default plugins that River applies to all jobs.
// This includes internal middleware like the resumable step middleware.
func DefaultPlugins() []rivertype.Plugin {
	return []rivertype.Plugin{&ResumableMiddleware{}}
}

// ResumableMiddleware is internal middleware that enables resumable step
// functionality. It reads the last completed step and cursor data from job
// metadata, injects them into the context, and persists updated step/cursor
// state back to metadata when a job errors after making progress.
type ResumableMiddleware struct{}

func (*ResumableMiddleware) IsMiddleware() bool { return true }
func (*ResumableMiddleware) IsPlugin() bool     { return true }

func (*ResumableMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	state := &ResumableState{
		AllStepNames:  make(map[string]struct{}),
		Cursors:       make(map[string]json.RawMessage),
		ResumeMatched: true,
		ResumeStep:    gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableStep).Str,
	}
	if state.ResumeStep != "" {
		state.ResumeMatched = false
	}

	if cursorJSON := gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableCursor); cursorJSON.Exists() && cursorJSON.Type == gjson.JSON {
		if err := json.Unmarshal([]byte(cursorJSON.Raw), &state.Cursors); err != nil {
			return fmt.Errorf("river: unmarshal resumable cursors: %w", err)
		}
	}

	// Cursor-only progress has no skip boundary. Track its targets separately
	// so a missing step cannot turn partially processed work into a success.
	// Older jobs with a step marker may contain stale cursors; preserve their
	// existing behavior rather than interpreting every cursor as unfinished.
	if state.ResumeStep == "" {
		state.PendingCursorSteps = slices.Sorted(maps.Keys(state.Cursors))
	}

	ctx = context.WithValue(ctx, ResumableContextKey{}, state)

	err := doInner(ctx)
	if err == nil {
		switch {
		case state.Err != nil:
			err = state.Err
		case state.ResumeStep != "" && !state.ResumeMatched:
			err = fmt.Errorf("river: resumable step %q not found in Worker", state.ResumeStep)
		case len(state.PendingCursorSteps) > 0:
			err = fmt.Errorf("river: resumable cursor step %q not found in Worker", state.PendingCursorSteps[0])
		}
	}

	// Advancing the skip boundary before finding the cursor could make that
	// step unreachable even after the worker restores it on a later attempt.
	if err != nil && len(state.PendingCursorSteps) == 0 {
		if state.CursorsDirty && len(state.Cursors) > 0 {
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = state.Cursors
		} else if state.CursorsDirty {
			// All cursors were consumed (their steps completed). Write null
			// to clear the stale cursor metadata, since the metadata merge
			// is additive and wouldn't remove the old key on its own.
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = nil
		}
		if state.CompletedStep != "" {
			metadataUpdates[rivercommon.MetadataKeyResumableStep] = state.CompletedStep
		}
	}

	return err
}

// ResumableState holds the state for a resumable job execution. It is stored in
// the context and accessed by ResumableStep and ResumableStepCursor.
type ResumableState struct {
	AllStepNames  map[string]struct{}
	CompletedStep string
	Cursors       map[string]json.RawMessage
	CursorsDirty  bool
	Err           error
	// PendingCursorSteps contains unmatched targets from cursor-only metadata.
	PendingCursorSteps []string
	ResumeMatched      bool
	ResumeStep         string
	StepActive         bool
	StepName           string
}

// ResumableContextKey is the context key for ResumableState.
type ResumableContextKey struct{}
