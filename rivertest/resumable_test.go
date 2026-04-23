package rivertest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type resumableTestArgs struct{}

func (resumableTestArgs) Kind() string { return "resumable_test" }

func TestResumableStepAfter(t *testing.T) {
	t.Parallel()

	t.Run("SetsStepInMetadata", func(t *testing.T) {
		t.Parallel()

		opts := &river.InsertOpts{}
		ResumableStepAfter(opts, "step2")

		var metadata map[string]any
		require.NoError(t, json.Unmarshal(opts.Metadata, &metadata))
		require.Equal(t, "step2", metadata[rivercommon.MetadataKeyResumableStep])
		require.NotContains(t, metadata, rivercommon.MetadataKeyResumableCursor)
	})

	t.Run("PreservesExistingMetadata", func(t *testing.T) {
		t.Parallel()

		opts := &river.InsertOpts{
			Metadata: []byte(`{"custom_key":"custom_value"}`),
		}
		ResumableStepAfter(opts, "step1")

		var metadata map[string]any
		require.NoError(t, json.Unmarshal(opts.Metadata, &metadata))
		require.Equal(t, "step1", metadata[rivercommon.MetadataKeyResumableStep])
		require.Equal(t, "custom_value", metadata["custom_key"])
	})
}

func TestResumableStepAtCursor(t *testing.T) {
	t.Parallel()

	type testCursor struct {
		LastProcessedID int `json:"last_processed_id"`
	}

	t.Run("SetsStepAndCursorInMetadata", func(t *testing.T) {
		t.Parallel()

		opts := &river.InsertOpts{}
		ResumableStepAtCursor(opts, "process_ids", testCursor{LastProcessedID: 42})

		var metadata map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(opts.Metadata, &metadata))

		var step string
		require.NoError(t, json.Unmarshal(metadata[rivercommon.MetadataKeyResumableStep], &step))
		require.Equal(t, "process_ids", step)

		var cursors map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(metadata[rivercommon.MetadataKeyResumableCursor], &cursors))

		var cursor testCursor
		require.NoError(t, json.Unmarshal(cursors["process_ids"], &cursor))
		require.Equal(t, 42, cursor.LastProcessedID)
	})
}

func TestResumableOptsIntegration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		return &testBundle{
			driver: riverpgxv5.New(nil),
			tx:     riverdbtest.TestTxPgx(ctx, t),
		}
	}

	t.Run("ResumableStepAfterSkipsCompletedSteps", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		var ran []string
		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[resumableTestArgs]) error {
			river.ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "step1")
				return nil
			})
			river.ResumableStep(ctx, "step2", nil, func(ctx context.Context) error {
				ran = append(ran, "step2")
				return nil
			})
			river.ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, "step3")
				return nil
			})
			return nil
		})

		config := &river.Config{ID: "rivertest-resumable"}
		tw := NewWorker(t, bundle.driver, config, worker)

		opts := &river.InsertOpts{}
		ResumableStepAfter(opts, "step1")

		result, err := tw.Work(ctx, t, bundle.tx, resumableTestArgs{}, opts)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
		require.Equal(t, []string{"step2", "step3"}, ran)
	})

	t.Run("ResumeAtCursorStepPassesCursor", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		type testCursor struct {
			LastProcessedID int `json:"last_processed_id"`
		}

		var (
			ran            []string
			receivedCursor testCursor
		)
		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[resumableTestArgs]) error {
			river.ResumableStep(ctx, "validate", nil, func(ctx context.Context) error {
				ran = append(ran, "validate")
				return nil
			})
			river.ResumableStepCursor(ctx, "process_ids", nil, func(ctx context.Context, cursor testCursor) error {
				ran = append(ran, "process_ids")
				receivedCursor = cursor
				return nil
			})
			return nil
		})

		config := &river.Config{ID: "rivertest-resumable"}
		tw := NewWorker(t, bundle.driver, config, worker)

		opts := &river.InsertOpts{}
		ResumableStepAtCursor(opts, "process_ids", testCursor{LastProcessedID: 42})

		result, err := tw.Work(ctx, t, bundle.tx, resumableTestArgs{}, opts)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
		require.Equal(t, []string{"process_ids"}, ran)
		require.Equal(t, 42, receivedCursor.LastProcessedID)
	})

	t.Run("ResumeAtFirstCursorStep", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		type testCursor struct {
			Offset int `json:"offset"`
		}

		var receivedCursor testCursor
		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[resumableTestArgs]) error {
			river.ResumableStepCursor(ctx, "process", nil, func(ctx context.Context, cursor testCursor) error {
				receivedCursor = cursor
				return nil
			})
			return nil
		})

		config := &river.Config{ID: "rivertest-resumable"}
		tw := NewWorker(t, bundle.driver, config, worker)

		opts := &river.InsertOpts{}
		ResumableStepAtCursor(opts, "process", testCursor{Offset: 100})

		result, err := tw.Work(ctx, t, bundle.tx, resumableTestArgs{}, opts)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
		require.Equal(t, 100, receivedCursor.Offset)
	})
}
