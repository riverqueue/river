package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
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

	t.Run("PanicsOutsideWorker", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithError(t, errResumableStepNotInWorker.Error(), func() {
			ResumableStep(context.Background(), "step1", func(ctx context.Context) error { return nil })
		})
	})

	t.Run("ResumesFromLastCompletedStep", func(t *testing.T) {
		t.Parallel()

		ctx, metadataUpdates, job := setup(t, `{}`)

		var ran []string
		err := (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", func(ctx context.Context) error {
				ran = append(ran, "step1")
				return nil
			})
			ResumableStep(ctx, "step2", func(ctx context.Context) error {
				ran = append(ran, "step2")
				return errors.New("step2 failed")
			})
			ResumableStep(ctx, "step3", func(ctx context.Context) error {
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

		err = (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", func(ctx context.Context) error {
				ran = append(ran, "step1")
				return nil
			})
			ResumableStep(ctx, "step2", func(ctx context.Context) error {
				ran = append(ran, "step2")
				return nil
			})
			ResumableStep(ctx, "step3", func(ctx context.Context) error {
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
		err := (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", func(ctx context.Context) error {
				ran = append(ran, "step1")
				cancel()
				return nil
			})
			ResumableStep(ctx, "step2", func(ctx context.Context) error {
				ran = append(ran, "step2")
				return ctx.Err()
			})
			ResumableStep(ctx, "step3", func(ctx context.Context) error {
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

	t.Run("ResumesCursor", func(t *testing.T) {
		t.Parallel()

		ctx, metadataUpdates, job := setup(t, `{}`)

		var (
			cursorResult resumableCursor
			ran          []int
		)
		err := (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", func(ctx context.Context) error {
				ran = append(ran, 1)
				return nil
			})
			ResumableStepCursor(ctx, "step2", func(ctx context.Context, cursor resumableCursor) error {
				cursorResult = cursor
				ran = append(ran, 2)
				require.NoError(t, ResumableSetCursor(ctx, resumableCursor{ID: 42}))
				return errors.New("step2 failed")
			})
			ResumableStep(ctx, "step3", func(ctx context.Context) error {
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

		err = (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStep(ctx, "step1", func(ctx context.Context) error {
				ran = append(ran, 1)
				return nil
			})
			ResumableStepCursor(ctx, "step2", func(ctx context.Context, cursor resumableCursor) error {
				cursorResult = cursor
				ran = append(ran, 2)
				return nil
			})
			ResumableStep(ctx, "step3", func(ctx context.Context) error {
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

	t.Run("SetCursorOutsideStep", func(t *testing.T) {
		t.Parallel()

		ctx, _, _ := setup(t, `{}`)

		err := (&resumableMiddleware{}).Work(ctx, &rivertype.JobRow{Metadata: []byte(`{}`)}, func(ctx context.Context) error {
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

		err := (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStepCursor(ctx, "step1", func(ctx context.Context, cursor int) error {
				require.Zero(t, cursor)
				require.NoError(t, ResumableSetCursor(ctx, 123))
				return nil
			})
			ResumableStepCursor(ctx, "step2", func(ctx context.Context, cursor secondCursor) error {
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

		err = (&resumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
			ResumableStepCursor(ctx, "step1", func(ctx context.Context, cursor int) error {
				require.Equal(t, 123, cursor)
				return nil
			})
			ResumableStepCursor(ctx, "step2", func(ctx context.Context, cursor secondCursor) error {
				require.Equal(t, secondCursor{ID: "abc"}, cursor)
				return nil
			})

			return nil
		})
		require.NoError(t, err)
		require.Empty(t, metadataUpdates)
	})
}
