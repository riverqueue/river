package river

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func Test_JobListPaginationCursor_JobListPaginationCursorFromJob(t *testing.T) {
	t.Parallel()

	for i, state := range []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStateRetryable,
		rivertype.JobStateScheduled,
	} {
		i, state := i, state

		t.Run(fmt.Sprintf("ScheduledAtUsedFor%sJob", state), func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			jobRow := &rivertype.JobRow{
				CreatedAt:   now.Add(-11 * time.Second),
				ID:          int64(i),
				Kind:        "test_kind",
				Queue:       "test_queue",
				State:       state,
				ScheduledAt: now.Add(-10 * time.Second),
			}

			cursor := JobListPaginationCursorFromJob(jobRow)
			require.Equal(t, jobRow.ID, cursor.id)
			require.Equal(t, jobRow.Kind, cursor.kind)
			require.Equal(t, jobRow.Queue, cursor.queue)
			require.Equal(t, jobRow.ScheduledAt, cursor.time)
		})
	}

	for i, state := range []rivertype.JobState{
		rivertype.JobStateCancelled,
		rivertype.JobStateCompleted,
		rivertype.JobStateDiscarded,
	} {
		i, state := i, state

		t.Run(fmt.Sprintf("FinalizedAtUsedFor%sJob", state), func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			jobRow := &rivertype.JobRow{
				AttemptedAt: ptrutil.Ptr(now.Add(-5 * time.Second)),
				CreatedAt:   now.Add(-11 * time.Second),
				FinalizedAt: ptrutil.Ptr(now.Add(-1 * time.Second)),
				ID:          int64(i),
				Kind:        "test_kind",
				Queue:       "test_queue",
				State:       state,
				ScheduledAt: now.Add(-10 * time.Second),
			}

			cursor := JobListPaginationCursorFromJob(jobRow)
			require.Equal(t, jobRow.ID, cursor.id)
			require.Equal(t, jobRow.Kind, cursor.kind)
			require.Equal(t, jobRow.Queue, cursor.queue)
			require.Equal(t, *jobRow.FinalizedAt, cursor.time)
		})
	}

	t.Run("RunningJobUsesAttemptedAt", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		jobRow := &rivertype.JobRow{
			AttemptedAt: ptrutil.Ptr(now.Add(-5 * time.Second)),
			CreatedAt:   now.Add(-11 * time.Second),
			ID:          4,
			Kind:        "test",
			Queue:       "test",
			State:       rivertype.JobStateRunning,
			ScheduledAt: now.Add(-10 * time.Second),
		}

		cursor := JobListPaginationCursorFromJob(jobRow)
		require.Equal(t, jobRow.ID, cursor.id)
		require.Equal(t, jobRow.Kind, cursor.kind)
		require.Equal(t, jobRow.Queue, cursor.queue)
		require.Equal(t, *jobRow.AttemptedAt, cursor.time)
	})

	t.Run("UnknownJobStateUsesCreatedAt", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		jobRow := &rivertype.JobRow{
			CreatedAt:   now.Add(-11 * time.Second),
			ID:          4,
			Kind:        "test",
			Queue:       "test",
			State:       rivertype.JobState("unknown_fake_state"),
			ScheduledAt: now.Add(-10 * time.Second),
		}

		cursor := JobListPaginationCursorFromJob(jobRow)
		require.Equal(t, jobRow.ID, cursor.id)
		require.Equal(t, jobRow.Kind, cursor.kind)
		require.Equal(t, jobRow.Queue, cursor.queue)
		require.Equal(t, jobRow.CreatedAt, cursor.time)
	})
}

func Test_JobListPaginationCursor_MarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("CanMarshalAndUnmarshal", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		params := &JobListPaginationCursor{
			id:    4,
			kind:  "test_kind",
			queue: "test_queue",
			time:  now,
		}

		text, err := json.Marshal(params)
		require.NoError(t, err)
		require.NotEqual(t, "", text)

		unmarshaledParams := &JobListPaginationCursor{}
		require.NoError(t, json.Unmarshal(text, unmarshaledParams))

		require.Equal(t, params, unmarshaledParams)
	})
}
