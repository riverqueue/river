package rivertest

import (
	"encoding/json"
	"fmt"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
)

// ResumableStepAfter configures insert options so that the job resumes after
// the named step. When the job is worked, all steps up to and including the
// named step will be skipped, and execution will begin at the next step.
//
// Used with [Worker.Work], simulates a job that previously completed the named
// step, then failed on a subsequent step and is now being retried.
//
//	result, err := testWorker.Work(ctx, t, tx, args,
//	    rivertest.ResumableStepAfter(&river.InsertOpts{}, "step1"))
func ResumableStepAfter(opts *river.InsertOpts, stepName string) *river.InsertOpts {
	mergeResumableMetadata(opts, stepName, nil)
	return opts
}

// ResumableStepAtCursor configures insert options so that the job resumes at
// the named step with the provided cursor data. The named step and all
// subsequent steps will execute, with all steps before it skipped.
//
// Used with [Worker.Work], simulates a job that was partway through a
// [river.ResumableStepCursor] step when it failed. On retry, the step runs
// again with the cursor so it can pick up where it left off.
//
//	result, err := testWorker.Work(ctx, t, tx, args,
//	    rivertest.ResumableStepAtCursor(&river.InsertOpts{}, "process_ids", MyCursor{LastID: 42}))
func ResumableStepAtCursor[TCursor any](opts *river.InsertOpts, stepName string, cursor TCursor) *river.InsertOpts {
	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		panic(fmt.Sprintf("rivertest: marshal resumable cursor: %s", err))
	}

	mergeResumableMetadata(opts, stepName, map[string]json.RawMessage{
		stepName: json.RawMessage(cursorBytes),
	})
	return opts
}

func mergeResumableMetadata(opts *river.InsertOpts, step string, cursors map[string]json.RawMessage) {
	var existing map[string]any
	if len(opts.Metadata) > 0 {
		if err := json.Unmarshal(opts.Metadata, &existing); err != nil {
			panic(fmt.Sprintf("rivertest: unmarshal existing metadata: %s", err))
		}
	} else {
		existing = make(map[string]any)
	}

	if step != "" {
		existing[rivercommon.MetadataKeyResumableStep] = step
	}
	if len(cursors) > 0 {
		existing[rivercommon.MetadataKeyResumableCursor] = cursors
	}

	metadataBytes, err := json.Marshal(existing)
	if err != nil {
		panic(fmt.Sprintf("rivertest: marshal resumable metadata: %s", err))
	}

	opts.Metadata = metadataBytes
}
