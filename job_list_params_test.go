package river

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
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
				AttemptedAt: new(now.Add(-5 * time.Second)),
				CreatedAt:   now.Add(-11 * time.Second),
				FinalizedAt: new(now.Add(-1 * time.Second)),
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
			AttemptedAt: new(now.Add(-5 * time.Second)),
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

func Test_JobListParams_toDBParams(t *testing.T) {
	t.Parallel()

	t.Run("FinalizedAtWithDefaultStates", func(t *testing.T) {
		t.Parallel()

		dbParams, err := NewJobListParams().OrderBy(JobListOrderByFinalizedAt, SortOrderAsc).toDBParams()
		require.NoError(t, err)
		require.Equal(t, []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		}, dbParams.States)
	})

	t.Run("FinalizedAtWithExplicitFinalizedStates", func(t *testing.T) {
		t.Parallel()

		dbParams, err := NewJobListParams().
			States(rivertype.JobStateCompleted).
			OrderBy(JobListOrderByFinalizedAt, SortOrderDesc).
			toDBParams()
		require.NoError(t, err)
		driverParams, err := dblist.JobMakeDriverParams(context.Background(), dbParams, riverpgxv5.New(nil))
		require.NoError(t, err)
		require.Equal(t, "completed", driverParams.NamedArgs["state"])
		require.Equal(t, "state = @state\n  AND finalized_at IS NOT NULL", driverParams.WhereClause)
	})

	t.Run("FinalizedAtWithMixedStates", func(t *testing.T) {
		t.Parallel()

		_, err := NewJobListParams().
			States(rivertype.JobStateAvailable, rivertype.JobStateCompleted).
			OrderBy(JobListOrderByFinalizedAt, SortOrderAsc).
			toDBParams()
		require.EqualError(t, err, "cannot order by finalized_at with non-finalized state filters [available]")
	})

	t.Run("FinalizedAtWithNonFinalizedStates", func(t *testing.T) {
		t.Parallel()

		_, err := NewJobListParams().
			States(rivertype.JobStatePending, rivertype.JobStateRunning).
			OrderBy(JobListOrderByFinalizedAt, SortOrderAsc).
			toDBParams()
		require.EqualError(t, err, "cannot order by finalized_at with non-finalized state filters [pending running]")
	})

	t.Run("FinalizedAtWithoutStates", func(t *testing.T) {
		t.Parallel()

		_, err := NewJobListParams().
			States().
			OrderBy(JobListOrderByFinalizedAt, SortOrderAsc).
			toDBParams()
		require.EqualError(t, err, "cannot order by finalized_at without finalized state filters")
	})

	t.Run("TagsAll", func(t *testing.T) {
		t.Parallel()

		tags := []string{"alpha", "beta"}
		params := NewJobListParams().TagsAll(tags...)
		tags[0] = "modified"

		dbParams, err := params.toDBParams()
		require.NoError(t, err)
		require.Equal(t, []string{"alpha", "beta"}, dbParams.TagsAll)

		dbParams, err = params.TagsAll("gamma").toDBParams()
		require.NoError(t, err)
		require.Equal(t, []string{"gamma"}, dbParams.TagsAll)

		dbParams, err = params.TagsAll().toDBParams()
		require.NoError(t, err)
		require.Empty(t, dbParams.TagsAll)
	})

	t.Run("TagsAny", func(t *testing.T) {
		t.Parallel()

		tags := []string{"alpha", "beta"}
		params := NewJobListParams().TagsAny(tags...)
		tags[0] = "modified"

		dbParams, err := params.toDBParams()
		require.NoError(t, err)
		require.Equal(t, []string{"alpha", "beta"}, dbParams.TagsAny)

		dbParams, err = params.TagsAny("gamma").toDBParams()
		require.NoError(t, err)
		require.Equal(t, []string{"gamma"}, dbParams.TagsAny)

		dbParams, err = params.TagsAny().toDBParams()
		require.NoError(t, err)
		require.Empty(t, dbParams.TagsAny)
	})
}

func Test_JobListParams_toDBParams_CustomConditions(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		driver *riverpgxv5.Driver
		params *JobListParams
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		return &testBundle{
			driver: riverpgxv5.New(nil),
			params: NewJobListParams().States(rivertype.JobStateCompleted).
				OrderBy(JobListOrderByTime, SortOrderDesc),
		}
	}

	driverParamsFunc := func(t *testing.T, bundle *testBundle) *riverdriver.JobListParams {
		t.Helper()

		dbParams, err := bundle.params.toDBParams()
		require.NoError(t, err)
		driverParams, err := dblist.JobMakeDriverParams(context.Background(), dbParams, bundle.driver)
		require.NoError(t, err)
		return driverParams
	}

	t.Run("ContradictoryFinalizedAt", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// A condition that excludes completed jobs is intentional; don't repair it.
		bundle.params = bundle.params.Where("finalized_at IS NULL")
		driverParams := driverParamsFunc(t, bundle)
		require.Equal(t, "state = any(@state)\n  AND finalized_at IS NULL", driverParams.WhereClause)
		require.Equal(t, map[string]any{"state": []string{"completed"}}, driverParams.NamedArgs)
	})

	t.Run("ContradictoryState", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Keep both state conditions, even though no row can satisfy both.
		bundle.params = bundle.params.Where("state = @other_state", NamedArgs{"other_state": "available"})
		driverParams := driverParamsFunc(t, bundle)
		require.Equal(t, "state = any(@state)\n  AND state = @other_state", driverParams.WhereClause)
		require.Equal(t, map[string]any{"other_state": "available", "state": []string{"completed"}}, driverParams.NamedArgs)
	})

	t.Run("DuplicateStateArgument", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// The typed state filter owns @state. A custom argument with the same
		// name must still report a conflict, rather than replacing the filter.
		bundle.params = bundle.params.Where("state = @state", NamedArgs{"state": "available"})
		dbParams, err := bundle.params.toDBParams()
		require.NoError(t, err)
		_, err = dblist.JobMakeDriverParams(context.Background(), dbParams, bundle.driver)
		require.EqualError(t, err, "named argument @state already registered")
	})

	t.Run("ExistingStateArgument", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Custom SQL can reuse @state, so its value must remain an array.
		bundle.params = bundle.params.Where("state = ANY(@state)")
		driverParams := driverParamsFunc(t, bundle)
		require.Equal(t, "state = any(@state)\n  AND state = ANY(@state)", driverParams.WhereClause)
		require.Equal(t, map[string]any{"state": []string{"completed"}}, driverParams.NamedArgs)
	})

	t.Run("GroupedOr", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		bundle.params = bundle.params.Where("(state = @other_state OR finalized_at IS NULL)", NamedArgs{"other_state": "completed"})
		driverParams := driverParamsFunc(t, bundle)
		require.Equal(t, "state = any(@state)\n  AND (state = @other_state OR finalized_at IS NULL)", driverParams.WhereClause)
		require.Equal(t, map[string]any{"other_state": "completed", "state": []string{"completed"}}, driverParams.NamedArgs)
	})

	t.Run("Metadata", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Metadata shares Where's internal condition list, so it also retains
		// the array filter and receives no inferred null condition.
		bundle.params = bundle.params.Metadata(`{"selected":true}`)
		driverParams := driverParamsFunc(t, bundle)
		require.Equal(t, "state = any(@state)\n  AND metadata @> @metadata_fragment::jsonb", driverParams.WhereClause)
		require.Equal(t, map[string]any{"metadata_fragment": `{"selected":true}`, "state": []string{"completed"}}, driverParams.NamedArgs)
	})

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Appending the cursor must preserve the custom OR's existing grouping
		// and leave its named argument independent of the cursor arguments.
		cursorTime := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
		bundle.params = bundle.params.
			Where("state = @other_state OR finalized_at IS NULL", NamedArgs{"other_state": "completed"}).
			After(&JobListCursor{id: 42, time: cursorTime})
		driverParams := driverParamsFunc(t, bundle)
		const cursorSQL = `("finalized_at" < @cursor_time OR ("finalized_at" = @cursor_time AND "id" < @after_id))`
		require.Equal(t, "state = any(@state)\n  AND state = @other_state OR finalized_at IS NULL\n  AND "+cursorSQL, driverParams.WhereClause)
		require.Equal(t, map[string]any{
			"after_id":    int64(42),
			"cursor_time": cursorTime,
			"other_state": "completed",
			"state":       []string{"completed"},
		}, driverParams.NamedArgs)
	})

	t.Run("RepeatedConversion", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		bundle.params = bundle.params.Where("state = ANY(@state)").
			After(&JobListCursor{id: 42, time: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)})

		// Conversion must not store generated conditions in the caller's params
		// or append another cursor each time the same params are used.
		first := driverParamsFunc(t, bundle)
		second := driverParamsFunc(t, bundle)
		require.Equal(t, first, second)
		require.Equal(t, []dblist.WherePredicate{{SQL: "state = ANY(@state)"}}, bundle.params.where)
	})

	t.Run("UngroupedOr", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// This OR can admit non-finalized rows despite the typed state filter.
		// Adding parentheses or a non-null condition would change the results.
		bundle.params = bundle.params.Where("false OR finalized_at IS NULL")
		driverParams := driverParamsFunc(t, bundle)
		require.Equal(t, "state = any(@state)\n  AND false OR finalized_at IS NULL", driverParams.WhereClause)
		require.Equal(t, map[string]any{"state": []string{"completed"}}, driverParams.NamedArgs)
	})
}

func Test_JobListParams_toDBParamsFinalizedIndex(t *testing.T) {
	t.Parallel()

	for _, state := range []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded} {
		for _, field := range []JobListOrderByField{JobListOrderByFinalizedAt, JobListOrderByTime} {
			for _, order := range []SortOrder{SortOrderAsc, SortOrderDesc} {
				t.Run(fmt.Sprintf("%s/%s/%d", state, field, order), func(t *testing.T) {
					t.Parallel()

					params := NewJobListParams().States(state).OrderBy(field, order)
					dbParams, err := params.toDBParams()
					require.NoError(t, err)
					driverParams, err := dblist.JobMakeDriverParams(context.Background(), dbParams, riverpgxv5.New(nil))
					require.NoError(t, err)
					require.Equal(t, "state = @state\n  AND finalized_at IS NOT NULL", driverParams.WhereClause)
					require.Equal(t, map[string]any{"state": string(state)}, driverParams.NamedArgs)
					direction := "ASC"
					if order == SortOrderDesc {
						direction = "DESC"
					}
					require.Equal(t, "finalized_at "+direction+", id "+direction, driverParams.OrderByClause)
					params = params.After(&JobListCursor{id: 42, time: time.Now().UTC()})
					dbParams, err = params.toDBParams()
					require.NoError(t, err)
					driverParams, err = dblist.JobMakeDriverParams(context.Background(), dbParams, riverpgxv5.New(nil))
					require.NoError(t, err)
					require.Len(t, dbParams.Where, 3)
					require.Equal(t, "state = @state\n  AND finalized_at IS NOT NULL\n  AND "+dbParams.Where[2].SQL, driverParams.WhereClause)
					require.Equal(t, map[string]any{"after_id": int64(42), "cursor_time": params.after.time, "state": string(state)}, driverParams.NamedArgs)
					require.Empty(t, params.where)
					repeated, err := params.toDBParams()
					require.NoError(t, err)
					require.Equal(t, dbParams, repeated)
				})
			}
		}
	}
}

func Test_JobListParams_toDBParamsWithoutFinalizedIndex(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		params *JobListParams
		where  string
	}{
		{"Default", NewJobListParams(), "state = any(@state)"},
		{"FinalizedByID", NewJobListParams().States(rivertype.JobStateCompleted), "state = @state"},
		{"FinalizedByScheduledAt", NewJobListParams().States(rivertype.JobStateCompleted).OrderBy(JobListOrderByScheduledAt, SortOrderAsc), "state = @state"},
		{"MixedStatesByTime", NewJobListParams().States(rivertype.JobStateCompleted, rivertype.JobStateAvailable).OrderBy(JobListOrderByTime, SortOrderDesc), "state = any(@state)"},
		{"MultipleFinalizedStates", NewJobListParams().OrderBy(JobListOrderByFinalizedAt, SortOrderAsc), "state = any(@state)"},
		{"MultipleNonFinalizedStates", NewJobListParams().States(rivertype.JobStateAvailable, rivertype.JobStateRunning).OrderBy(JobListOrderByTime, SortOrderDesc), "state = any(@state)"},
		{"NonFinalized", NewJobListParams().States(rivertype.JobStateAvailable).OrderBy(JobListOrderByTime, SortOrderAsc), "state = @state"},
		{"Unfiltered", NewJobListParams().States(), "true"},
		{"Unknown", NewJobListParams().States("unknown").OrderBy(JobListOrderByFinalizedAt, SortOrderAsc), "state = @state"},
		{"UnknownAndFinalized", NewJobListParams().States(rivertype.JobStateCompleted, "unknown").OrderBy(JobListOrderByTime, SortOrderDesc), "state = any(@state)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dbParams, err := tt.params.toDBParams()
			require.NoError(t, err)
			driverParams, err := dblist.JobMakeDriverParams(context.Background(), dbParams, riverpgxv5.New(nil))
			require.NoError(t, err)
			require.Equal(t, tt.where, driverParams.WhereClause)
		})
	}
}
