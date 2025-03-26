package river

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func Test_JobListCursor_JobListCursorFromJob(t *testing.T) {
	t.Parallel()

	jobRow := &rivertype.JobRow{
		ID:    4,
		Kind:  "test",
		Queue: "test",
		State: rivertype.JobStateRunning,
	}

	cursor := JobListCursorFromJob(jobRow)
	require.Zero(t, cursor.id)
	require.Equal(t, jobRow, cursor.job)
	require.Empty(t, cursor.kind)
	require.Empty(t, cursor.queue)
	require.Zero(t, cursor.sortField)
	require.Zero(t, cursor.time)
}

func Test_JobListCursor_jobListCursorFromJobAndParams(t *testing.T) {
	t.Parallel()

	t.Run("OrderByID", func(t *testing.T) {
		t.Parallel()

		jobRow := &rivertype.JobRow{
			ID:    4,
			Kind:  "test",
			Queue: "test",
			State: rivertype.JobStateRunning,
		}

		cursor := jobListCursorFromJobAndParams(jobRow, NewJobListParams().After(JobListCursorFromJob(jobRow)))
		require.Equal(t, jobRow.ID, cursor.id)
		require.Equal(t, jobRow.Kind, cursor.kind)
		require.Equal(t, jobRow.Queue, cursor.queue)
		require.Zero(t, cursor.time)
	})

	for i, state := range []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStateRetryable,
		rivertype.JobStateScheduled,
	} {
		t.Run(fmt.Sprintf("OrderByTimeScheduledAtUsedFor%sJob", state), func(t *testing.T) {
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

			cursor := jobListCursorFromJobAndParams(jobRow, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc))
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
		t.Run(fmt.Sprintf("OrderByTimeFinalizedAtUsedFor%sJob", state), func(t *testing.T) {
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

			cursor := jobListCursorFromJobAndParams(jobRow, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc))
			require.Equal(t, jobRow.ID, cursor.id)
			require.Equal(t, jobRow.Kind, cursor.kind)
			require.Equal(t, jobRow.Queue, cursor.queue)
			require.Equal(t, *jobRow.FinalizedAt, cursor.time)
		})
	}

	t.Run("OrderByTimeRunningJobUsesAttemptedAt", func(t *testing.T) {
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

		cursor := jobListCursorFromJobAndParams(jobRow, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc))
		require.Equal(t, jobRow.ID, cursor.id)
		require.Equal(t, jobRow.Kind, cursor.kind)
		require.Equal(t, jobRow.Queue, cursor.queue)
		require.Equal(t, *jobRow.AttemptedAt, cursor.time)
	})

	t.Run("OrderByTimeUnknownJobStateUsesCreatedAt", func(t *testing.T) {
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

		cursor := jobListCursorFromJobAndParams(jobRow, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc))
		require.Equal(t, jobRow.ID, cursor.id)
		require.Equal(t, jobRow.Kind, cursor.kind)
		require.Equal(t, jobRow.Queue, cursor.queue)
		require.Equal(t, jobRow.CreatedAt, cursor.time)
	})
}

func Test_JobListCursor_MarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("CanMarshalAndUnmarshal", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		cursor := &JobListCursor{
			id:    4,
			kind:  "test_kind",
			queue: "test_queue",
			time:  now,
		}

		text, err := json.Marshal(cursor)
		require.NoError(t, err)
		require.NotEmpty(t, text)

		unmarshaledParams := &JobListCursor{}
		require.NoError(t, json.Unmarshal(text, unmarshaledParams))

		require.Equal(t, cursor, unmarshaledParams)
	})

	t.Run("ErrorsOnJobOnlyCursor", func(t *testing.T) {
		t.Parallel()

		jobRow := &rivertype.JobRow{
			ID:    4,
			Kind:  "test",
			Queue: "test",
			State: rivertype.JobStateRunning,
		}

		cursor := JobListCursorFromJob(jobRow)

		_, err := json.Marshal(cursor)
		require.EqualError(t, err, "json: error calling MarshalText for type *river.JobListCursor: cursor initialized with only a job can't be marshaled; try a cursor from JobListResult instead")
	})
}
