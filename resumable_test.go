package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	"github.com/riverqueue/river/rivertype"
)

func TestResumableNestedStep(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		childCursor  bool
		parentCursor bool
		parentName   string
	}{
		{name: "CursorInCursor", childCursor: true, parentCursor: true, parentName: "parent"},
		{name: "CursorInPlain", childCursor: true, parentName: "parent"},
		{name: "EmptyParentName"},
		{name: "PlainInCursor", parentCursor: true, parentName: "parent"},
		{name: "PlainInPlain", parentName: "parent"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			updates := make(map[string]any)
			ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, updates)
			job := &rivertype.JobRow{Metadata: []byte(`{}`)}

			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStep(ctx, "before", nil, func(context.Context) error { return nil })
				parentFunc := func(ctx context.Context) {
					if testCase.childCursor {
						ResumableStepCursor(ctx, "child", nil, func(context.Context, int) error {
							t.Fatal("nested child must not run")
							return nil
						})
					} else {
						ResumableStep(ctx, "child", nil, func(context.Context) error {
							t.Fatal("nested child must not run")
							return nil
						})
					}
				}
				if testCase.parentCursor {
					ResumableStepCursor(ctx, testCase.parentName, nil, func(ctx context.Context, _ int) error {
						require.NoError(t, ResumableSetCursor(ctx, 1))
						parentFunc(ctx)
						return nil
					})
				} else {
					ResumableStep(ctx, testCase.parentName, nil, func(ctx context.Context) error {
						parentFunc(ctx)
						return nil
					})
				}
				ResumableStep(ctx, "after", nil, func(context.Context) error {
					t.Fatal("steps after the nesting error must not run")
					return nil
				})
				return nil
			})
			require.EqualError(t, err, "river: resumable steps cannot be nested")
			metadata, err := json.Marshal(updates)
			require.NoError(t, err)
			if testCase.parentCursor {
				require.JSONEq(t, `{"river:resumable_step":"before","river:resumable_cursor":{"parent":1}}`, string(metadata))
			} else {
				require.JSONEq(t, `{"river:resumable_step":"before"}`, string(metadata))
			}
		})
	}
}

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

	for _, outcome := range []string{"Cancel", "Error", "Shutdown", "Snooze"} {
		t.Run("FirstStepCursorOn"+outcome, func(t *testing.T) {
			t.Parallel()

			ctx, updates, job := setup(t, `{}`)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStepCursor(ctx, "loop", nil, func(ctx context.Context, cursor resumableCursor) error {
					require.Zero(t, cursor.ID)
					require.NoError(t, ResumableSetCursor(ctx, resumableCursor{ID: 7}))
					switch outcome {
					case "Cancel":
						return JobCancel(errors.New("cancel"))
					case "Shutdown":
						cancel()
						return ctx.Err()
					case "Snooze":
						return JobSnooze(time.Minute)
					default:
						return errors.New("retry")
					}
				})
				return nil
			})
			require.Error(t, err)
			encoded, err := json.Marshal(updates)
			require.NoError(t, err)
			require.JSONEq(t, `{"river:resumable_cursor":{"loop":{"id":7}}}`, string(encoded))

			// Without a completed step, start from the beginning and supply the
			// saved cursor when its named step is reached.
			ctx, updates, job = setup(t, string(encoded))
			err = (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStepCursor(ctx, "loop", nil, func(ctx context.Context, cursor resumableCursor) error {
					require.Equal(t, 7, cursor.ID)
					return nil
				})
				return errors.New("later failure")
			})
			require.EqualError(t, err, "later failure")
			require.Equal(t, "loop", updates[rivercommon.MetadataKeyResumableStep])
			require.Contains(t, updates, rivercommon.MetadataKeyResumableCursor)
			require.Nil(t, updates[rivercommon.MetadataKeyResumableCursor])
		})
	}

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
