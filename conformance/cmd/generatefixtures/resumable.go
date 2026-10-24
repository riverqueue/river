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
