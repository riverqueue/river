package main

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func TestMakeResumableCases(t *testing.T) {
	t.Parallel()

	fixture, err := makeResumableCases()
	require.NoError(t, err)
	want := map[string]string{
		"checkpoint_commit_error":      `{"river:resumable_step":"loop","river:resumable_cursor":{"loop":1}}`,
		"checkpoint_then_cursor_error": `{"river:resumable_step":"loop","river:resumable_cursor":{"loop":2}}`,
		"checkpoint_rollback_error":    `{}`,
		"checkpoint_write_failure":     `{"river:resumable_step":"first"}`,
		"first_cursor_error":           `{"river:resumable_cursor":{"loop":7}}`,
		"step_snooze":                  `{"river:resumable_step":"first"}`,
	}
	require.Len(t, fixture.Cases, len(want))
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Parallel()

			metadata, err := json.Marshal(testCase.ExpectedMetadata)
			require.NoError(t, err)
			require.JSONEq(t, want[testCase.Name], string(metadata))
		})
	}
}

func TestResumableCursorOnly(t *testing.T) {
	t.Parallel()

	for _, testCase := range makeResumableCursorOnlyCases().Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Parallel()

			metadata, err := json.Marshal(testCase.Metadata)
			require.NoError(t, err)
			updates := make(map[string]any)
			ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, updates)
			client, err := river.NewClient(&fixtureCheckpointDriver{}, &river.Config{})
			require.NoError(t, err)
			ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
			row := &rivertype.JobRow{ID: 1, EncodedArgs: []byte(`{}`), Metadata: metadata, State: rivertype.JobStateRunning}
			staged := *row
			exec := &fixtureCheckpointExecutor{row: &staged}
			ran := []string{}

			err = (&riverplugin.ResumableMiddleware{}).Work(ctx, row, func(ctx context.Context) error {
				for _, action := range testCase.Actions {
					actionFunc := func(ctx context.Context) error {
						ran = append(ran, action.Name)
						if action.Kind == "checkpoint" {
							_, err := river.ResumableSetStepTx[*fixtureCheckpointDriver](ctx, riverdriver.ExecutorTx(exec), &river.Job[fixtureResumableArgs]{JobRow: row})
							return err
						}
						if action.SetCursor {
							require.NoError(t, river.ResumableSetCursor(ctx, 7))
						}
						if action.Fail {
							return errors.New("step interrupted")
						}
						return nil
					}
					if action.Kind == "cursor" {
						river.ResumableStepCursor(ctx, action.Name, nil, func(ctx context.Context, cursor any) error {
							cursors, ok := testCase.Metadata[rivercommon.MetadataKeyResumableCursor].(map[string]any)
							require.True(t, ok)
							initial := cursors[action.Name]
							actual, err := json.Marshal(cursor)
							require.NoError(t, err)
							expected, err := json.Marshal(initial)
							require.NoError(t, err)
							require.JSONEq(t, string(expected), string(actual))
							return actionFunc(ctx)
						})
					} else {
						river.ResumableStep(ctx, action.Name, nil, actionFunc)
					}
				}
				if testCase.WorkerError {
					return errors.New("worker interrupted")
				}
				return nil
			})
			if testCase.ExpectedError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.ExpectedError)
			}
			require.Equal(t, testCase.ExpectedRuns, ran)
			actual, err := json.Marshal(updates)
			require.NoError(t, err)
			expected, err := json.Marshal(testCase.ExpectedUpdates)
			require.NoError(t, err)
			require.JSONEq(t, string(expected), string(actual))
			require.Equal(t, metadata, staged.Metadata, "a checkpoint before the target must not write")

			if testCase.ExpectedError != "" && len(testCase.ExpectedUpdates) == 0 {
				// Failed validation must leave a retry able to find the restored step.
				err = (&riverplugin.ResumableMiddleware{}).Work(ctx, row, func(ctx context.Context) error {
					cursors, ok := testCase.Metadata[rivercommon.MetadataKeyResumableCursor].(map[string]any)
					require.True(t, ok)
					for _, name := range slices.Sorted(maps.Keys(cursors)) {
						river.ResumableStepCursor(ctx, name, nil, func(context.Context, any) error { return nil })
					}
					return nil
				})
				require.NoError(t, err)
			}
		})
	}
}
