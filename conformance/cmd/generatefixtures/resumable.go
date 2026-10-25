package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// fixtureCheckpointDriver only supplies the transactional update boundary. The
// staged row is committed or discarded by each scenario, like a caller's tx.
type fixtureCheckpointDriver struct {
	riverdriver.Driver[riverdriver.ExecutorTx]
}

func (*fixtureCheckpointDriver) UnwrapExecutor(tx riverdriver.ExecutorTx) riverdriver.ExecutorTx {
	return tx
}

type fixtureCheckpointExecutor struct {
	riverdriver.ExecutorTx

	fail bool
	row  *rivertype.JobRow
}

func (e *fixtureCheckpointExecutor) JobUpdate(_ context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	if e.fail {
		return nil, errors.New("checkpoint write failed")
	}
	e.row.Metadata = params.Metadata
	return e.row, nil
}

type fixtureResumableArgs struct{}

func (fixtureResumableArgs) Kind() string { return "resumable_fixture" }

type resumableCase struct {
	ExpectedMetadata map[string]any `json:"expected_metadata"`
	Name             string         `json:"name"`
}

type resumableCases struct {
	Cases   []resumableCase `json:"cases"`
	Comment string          `json:"$comment"`
}

type resumableCursorOnlyAction struct {
	Fail      bool   `json:"fail"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	SetCursor bool   `json:"set_cursor"`
}

type resumableCursorOnlyCase struct {
	Actions         []resumableCursorOnlyAction `json:"actions"`
	ExpectedError   string                      `json:"expected_error"`
	ExpectedRuns    []string                    `json:"expected_runs"`
	ExpectedUpdates map[string]any              `json:"expected_updates"`
	Metadata        map[string]any              `json:"metadata"`
	Name            string                      `json:"name"`
	WorkerError     bool                        `json:"worker_error"`
}

type resumableCursorOnlyCases struct {
	Cases   []resumableCursorOnlyCase `json:"cases"`
	Comment string                    `json:"$comment"`
}

func makeResumableCases() (resumableCases, error) {
	fixture := resumableCases{Comment: generatedComment("River resumable steps and transactional checkpoints")}
	for _, name := range []string{
		"checkpoint_commit_error", "checkpoint_rollback_error", "checkpoint_then_cursor_error", "checkpoint_write_failure",
		"first_cursor_error", "step_snooze",
	} {
		updates := make(map[string]any)
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, updates)
		client, err := river.NewClient(&fixtureCheckpointDriver{}, &river.Config{})
		if err != nil {
			return fixture, err
		}
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
		row := &rivertype.JobRow{ID: 1, EncodedArgs: []byte(`{}`), Metadata: []byte(`{}`), State: rivertype.JobStateRunning}
		staged := *row
		exec := &fixtureCheckpointExecutor{fail: name == "checkpoint_write_failure", row: &staged}
		attemptErr := (&riverplugin.ResumableMiddleware{}).Work(ctx, row, func(ctx context.Context) error {
			if name != "first_cursor_error" {
				river.ResumableStep(ctx, "first", nil, func(context.Context) error { return nil })
			}
			if name == "step_snooze" {
				return river.JobSnooze(60_000_000_000)
			}
			river.ResumableStepCursor(ctx, "loop", nil, func(ctx context.Context, _ int) error {
				if name == "first_cursor_error" {
					if err := river.ResumableSetCursor(ctx, 7); err != nil {
						return err
					}
				} else {
					if _, err := river.ResumableSetStepCursorTx[*fixtureCheckpointDriver](ctx, riverdriver.ExecutorTx(exec), &river.Job[fixtureResumableArgs]{JobRow: row}, 1); err != nil {
						return err
					}
				}
				if name == "checkpoint_then_cursor_error" {
					if err := river.ResumableSetCursor(ctx, 2); err != nil {
						return err
					}
				}
				return errors.New("retry")
			})
			return nil
		})
		if attemptErr == nil {
			return fixture, fmt.Errorf("resumable scenario %s unexpectedly succeeded", name)
		}
		metadata := make(map[string]any)
		if name == "checkpoint_commit_error" || name == "checkpoint_then_cursor_error" {
			if err := json.Unmarshal(staged.Metadata, &metadata); err != nil {
				return fixture, err
			}
		}
		maps.Copy(metadata, updates)
		fixture.Cases = append(fixture.Cases, resumableCase{ExpectedMetadata: metadata, Name: name})
	}
	return fixture, nil
}

func makeResumableCursorOnlyCases() resumableCursorOnlyCases {
	const (
		cursorKey = "river:resumable_cursor"
		stepKey   = "river:resumable_step"
	)
	cursorOnly := func(value any) map[string]any {
		return map[string]any{cursorKey: map[string]any{"loop": value}}
	}
	cases := []resumableCursorOnlyCase{
		{Actions: []resumableCursorOnlyAction{{Kind: "checkpoint", Name: "later"}}, ExpectedError: `resumable checkpoint cannot precede saved cursor step "loop"`, ExpectedRuns: []string{"later"}, Metadata: cursorOnly(0), Name: "CheckpointBeforeCursor"},
		{Actions: []resumableCursorOnlyAction{{Fail: true, Kind: "plain", Name: "before"}}, ExpectedError: "step interrupted", ExpectedRuns: []string{"before"}, Metadata: cursorOnly(0), Name: "EarlierError"},
		{Actions: []resumableCursorOnlyAction{{Kind: "cursor", Name: "loop"}}, ExpectedError: "worker interrupted", ExpectedRuns: []string{"loop"}, ExpectedUpdates: map[string]any{cursorKey: nil, stepKey: "loop"}, Metadata: map[string]any{cursorKey: map[string]any{"loop": nil}, stepKey: ""}, Name: "EmptyStepMarker", WorkerError: true},
		{Actions: []resumableCursorOnlyAction{{Kind: "plain", Name: "done"}, {Kind: "plain", Name: "later"}}, ExpectedRuns: []string{"later"}, Metadata: map[string]any{cursorKey: map[string]any{"loop": 0}, stepKey: "done"}, Name: "LegacyStaleCursor"},
		{Actions: []resumableCursorOnlyAction{{Kind: "plain", Name: "later"}}, ExpectedError: `resumable cursor step "loop" not found`, ExpectedRuns: []string{"later"}, Metadata: cursorOnly(0), Name: "MissingAfterLaterStep"},
		{Actions: []resumableCursorOnlyAction{{Kind: "cursor", Name: "new", SetCursor: true}}, ExpectedError: `resumable cursor step "loop" not found`, ExpectedRuns: []string{"new"}, Metadata: cursorOnly(0), Name: "MissingAfterNewCursor"},
		{ExpectedError: `resumable cursor step "loop" not found`, Metadata: cursorOnly(nil), Name: "MissingNull"},
		{ExpectedError: `resumable cursor step "loop" not found`, Metadata: cursorOnly(0), Name: "MissingZero"},
		{Actions: []resumableCursorOnlyAction{{Kind: "cursor", Name: "b"}, {Kind: "cursor", Name: "a"}}, ExpectedError: "worker interrupted", ExpectedRuns: []string{"b", "a"}, ExpectedUpdates: map[string]any{cursorKey: nil, stepKey: "a"}, Metadata: map[string]any{cursorKey: map[string]any{"a": 0, "b": nil}}, Name: "MultipleCursors", WorkerError: true},
		{Actions: []resumableCursorOnlyAction{{Kind: "cursor", Name: "loop"}}, ExpectedError: "worker interrupted", ExpectedRuns: []string{"loop"}, ExpectedUpdates: map[string]any{cursorKey: nil, stepKey: "loop"}, Metadata: cursorOnly(nil), Name: "NullValueResumes", WorkerError: true},
		{Actions: []resumableCursorOnlyAction{{Kind: "plain", Name: "loop"}}, ExpectedError: `resumable cursor step "loop" requires a cursor step`, Metadata: cursorOnly(0), Name: "PlainReplacement"},
		{Actions: []resumableCursorOnlyAction{{Kind: "plain", Name: "before"}, {Kind: "cursor", Name: "loop"}}, ExpectedError: "worker interrupted", ExpectedRuns: []string{"before", "loop"}, ExpectedUpdates: map[string]any{cursorKey: nil, stepKey: "loop"}, Metadata: cursorOnly(0), Name: "ResumesAfterEarlierStep", WorkerError: true},
		{Actions: []resumableCursorOnlyAction{{Kind: "plain", Name: "before"}, {Fail: true, Kind: "cursor", Name: "loop", SetCursor: true}}, ExpectedError: "step interrupted", ExpectedRuns: []string{"before", "loop"}, ExpectedUpdates: map[string]any{cursorKey: map[string]any{"loop": 7}, stepKey: "before"}, Metadata: cursorOnly(0), Name: "TargetError"},
		{Actions: []resumableCursorOnlyAction{{Kind: "plain", Name: "later"}}, ExpectedError: "worker interrupted", ExpectedRuns: []string{"later"}, Metadata: cursorOnly(0), Name: "WorkerError", WorkerError: true},
		{Actions: []resumableCursorOnlyAction{{Kind: "cursor", Name: "loop"}}, ExpectedError: "worker interrupted", ExpectedRuns: []string{"loop"}, ExpectedUpdates: map[string]any{cursorKey: nil, stepKey: "loop"}, Metadata: cursorOnly(0), Name: "ZeroValueResumes", WorkerError: true},
	}
	for i := range cases {
		if cases[i].Actions == nil {
			cases[i].Actions = []resumableCursorOnlyAction{}
		}
		if cases[i].ExpectedRuns == nil {
			cases[i].ExpectedRuns = []string{}
		}
		if cases[i].ExpectedUpdates == nil {
			cases[i].ExpectedUpdates = map[string]any{}
		}
	}
	return resumableCursorOnlyCases{Cases: cases, Comment: generatedComment("River cursor-only checkpoint compatibility and validation")}
}
