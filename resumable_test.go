package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	"github.com/riverqueue/river/rivertype"
)

func TestResumableStep(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, metadata string) (context.Context, map[string]any, *rivertype.JobRow) {
		t.Helper()

		metadataUpdates := make(map[string]any)
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		return ctx, metadataUpdates, &rivertype.JobRow{Metadata: []byte(metadata)}
	}

	t.Run("DuplicateStepName", func(t *testing.T) {
		t.Parallel()

		ctx, _, job := setup(t, `{}`)

		var ran []string
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "first")
				return nil
			})
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "second")
				return nil
			})

			return nil
		})
		require.EqualError(t, err, `river: duplicate resumable step name "step1"`)
		require.Equal(t, []string{"first"}, ran)
	})

	t.Run("DuplicateStepNameWhenSkippingCompletedSteps", func(t *testing.T) {
		t.Parallel()

		ctx, _, job := setup(t, `{"river:resumable_step":"step2"}`)

		var ran []string
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "first")
				return nil
			})
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "second")
				return nil
			})
			ResumableStep(ctx, "step2", nil, func(ctx context.Context) error {
				ran = append(ran, "third")
				return nil
			})

			return nil
		})
		require.EqualError(t, err, `river: duplicate resumable step name "step1"`)
		require.Empty(t, ran)
	})

	t.Run("PanicsOutsideWorker", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithError(t, errResumableStepNotInWorker.Error(), func() {
			ResumableStep(context.Background(), "step1", nil, func(ctx context.Context) error { return nil })
		})
	})

	t.Run("ResumesFromLastCompletedStep", func(t *testing.T) {
		t.Parallel()

		ctx, metadataUpdates, job := setup(t, `{}`)

		var ran []string
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "step1")
				return nil
			})
			ResumableStep(ctx, "step2", nil, func(ctx context.Context) error {
				ran = append(ran, "step2")
				return errors.New("step2 failed")
			})
			ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, "step3")
				return nil
			})

			return nil
		})
		require.EqualError(t, err, "step2 failed")
		require.Equal(t, []string{"step1", "step2"}, ran)
		require.Equal(t, "step1", metadataUpdates[rivercommon.MetadataKeyResumableStep])

		ctx, metadataUpdates, job = setup(t, `{"river:resumable_step":"step1"}`)
		ran = nil

		err = (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "step1")
				return nil
			})
			ResumableStep(ctx, "step2", nil, func(ctx context.Context) error {
				ran = append(ran, "step2")
				return nil
			})
			ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, "step3")
				return nil
			})

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []string{"step2", "step3"}, ran)
		require.Empty(t, metadataUpdates)
	})

	t.Run("SavesLastCompletedStepOnContextCancellation", func(t *testing.T) {
		t.Parallel()

		baseCtx, metadataUpdates, job := setup(t, `{}`)
		ctx, cancel := context.WithCancel(baseCtx)
		defer cancel()

		var ran []string
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "step1")
				cancel()
				return nil
			})
			ResumableStep(ctx, "step2", nil, func(ctx context.Context) error {
				ran = append(ran, "step2")
				return ctx.Err()
			})
			ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, "step3")
				return nil
			})

			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, []string{"step1", "step2"}, ran)
		require.Equal(t, "step1", metadataUpdates[rivercommon.MetadataKeyResumableStep])
	})
}

func TestResumableStepCursor(t *testing.T) {
	t.Parallel()

	type resumableCursor struct {
		ID int `json:"id"`
	}

	setup := func(t *testing.T, metadata string) (context.Context, map[string]any, *rivertype.JobRow) {
		t.Helper()

		metadataUpdates := make(map[string]any)
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		return ctx, metadataUpdates, &rivertype.JobRow{Metadata: []byte(metadata)}
	}

	t.Run("DuplicateStepNameSharedWithCursorStep", func(t *testing.T) {
		t.Parallel()

		ctx, _, job := setup(t, `{}`)

		var ran []string
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, "step")
				return nil
			})
			ResumableStepCursor(ctx, "step1", nil, func(ctx context.Context, cursor resumableCursor) error {
				ran = append(ran, "cursor")
				return nil
			})

			return nil
		})
		require.EqualError(t, err, `river: duplicate resumable step name "step1"`)
		require.Equal(t, []string{"step"}, ran)
	})

	t.Run("ResumesCursor", func(t *testing.T) {
		t.Parallel()

		ctx, metadataUpdates, job := setup(t, `{}`)

		var (
			cursorResult resumableCursor
			ran          []int
		)
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, 1)
				return nil
			})
			ResumableStepCursor(ctx, "step2", nil, func(ctx context.Context, cursor resumableCursor) error {
				cursorResult = cursor
				ran = append(ran, 2)
				require.NoError(t, ResumableSetCursor(ctx, resumableCursor{ID: 42}))
				return errors.New("step2 failed")
			})
			ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, 3)
				return nil
			})

			return nil
		})
		require.EqualError(t, err, "step2 failed")
		require.Equal(t, resumableCursor{}, cursorResult)
		require.Equal(t, []int{1, 2}, ran)
		require.Equal(t, "step1", metadataUpdates[rivercommon.MetadataKeyResumableStep])
		cursorMetadata, err := json.Marshal(metadataUpdates[rivercommon.MetadataKeyResumableCursor])
		require.NoError(t, err)
		require.JSONEq(t, `{"step2":{"id":42}}`, string(cursorMetadata))

		ctx, metadataUpdates, job = setup(t, `{"river:resumable_cursor":{"step2":{"id":42}},"river:resumable_step":"step1"}`)
		cursorResult = resumableCursor{}
		ran = nil

		err = (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, 1)
				return nil
			})
			ResumableStepCursor(ctx, "step2", nil, func(ctx context.Context, cursor resumableCursor) error {
				cursorResult = cursor
				ran = append(ran, 2)
				return nil
			})
			ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, 3)
				return nil
			})

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, resumableCursor{ID: 42}, cursorResult)
		require.Equal(t, []int{2, 3}, ran)
		require.Empty(t, metadataUpdates)
	})

	t.Run("CursorStepCompletedThenLaterStepFails_ClearsStaleMetadata", func(t *testing.T) {
		t.Parallel()

		// Simulate: cursor step2 previously failed with a cursor, now
		// resuming. step2 succeeds this time, but step3 fails.
		ctx, metadataUpdates, job := setup(t, `{"river:resumable_cursor":{"step2":{"id":42}},"river:resumable_step":"step1"}`)

		var ran []int
		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", nil, func(ctx context.Context) error {
				ran = append(ran, 1)
				return nil
			})
			ResumableStepCursor(ctx, "step2", nil, func(ctx context.Context, cursor resumableCursor) error {
				ran = append(ran, 2)
				return nil
			})
			ResumableStep(ctx, "step3", nil, func(ctx context.Context) error {
				ran = append(ran, 3)
				return errors.New("step3 failed")
			})

			return nil
		})
		require.EqualError(t, err, "step3 failed")
		require.Equal(t, []int{2, 3}, ran)

		// completedStep should be step2 (step3 failed).
		require.Equal(t, "step2", metadataUpdates[rivercommon.MetadataKeyResumableStep])

		// Cursor metadata must be explicitly cleared (set to nil) so the
		// stale cursor for step2 doesn't persist through the additive
		// metadata merge. Without this, the next retry would see the old
		// cursor and re-run step2 instead of skipping it.
		require.Contains(t, metadataUpdates, rivercommon.MetadataKeyResumableCursor)
		require.Nil(t, metadataUpdates[rivercommon.MetadataKeyResumableCursor])
	})

	t.Run("SetCursorOutsideStep", func(t *testing.T) {
		t.Parallel()

		ctx, _, _ := setup(t, `{}`)

		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, &rivertype.JobRow{Metadata: []byte(`{}`)}, func(ctx context.Context) error {
			return ResumableSetCursor(ctx, 1)
		})
		require.ErrorIs(t, err, errResumableCursorNotInStep)
	})

	t.Run("SupportsMultipleCursorStepsWithDifferentTypes", func(t *testing.T) {
		t.Parallel()

		type secondCursor struct {
			ID string `json:"id"`
		}

		ctx, metadataUpdates, job := setup(t, `{}`)

		err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStepCursor(ctx, "step1", nil, func(ctx context.Context, cursor int) error {
				require.Zero(t, cursor)
				require.NoError(t, ResumableSetCursor(ctx, 123))
				return nil
			})
			ResumableStepCursor(ctx, "step2", nil, func(ctx context.Context, cursor secondCursor) error {
				require.Equal(t, secondCursor{}, cursor)
				require.NoError(t, ResumableSetCursor(ctx, secondCursor{ID: "abc"}))
				return errors.New("step2 failed")
			})

			return nil
		})
		require.EqualError(t, err, "step2 failed")
		require.Equal(t, "step1", metadataUpdates[rivercommon.MetadataKeyResumableStep])
		cursorMetadata, err := json.Marshal(metadataUpdates[rivercommon.MetadataKeyResumableCursor])
		require.NoError(t, err)
		require.JSONEq(t, `{"step2":{"id":"abc"}}`, string(cursorMetadata))

		ctx, metadataUpdates, job = setup(t, `{"river:resumable_cursor":{"step1":123,"step2":{"id":"abc"}},"river:resumable_step":"step1"}`)

		err = (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStepCursor(ctx, "step1", nil, func(ctx context.Context, cursor int) error {
				require.Equal(t, 123, cursor)
				return nil
			})
			ResumableStepCursor(ctx, "step2", nil, func(ctx context.Context, cursor secondCursor) error {
				require.Equal(t, secondCursor{ID: "abc"}, cursor)
				return nil
			})

			return nil
		})
		require.NoError(t, err)
		require.Empty(t, metadataUpdates)
	})
}
