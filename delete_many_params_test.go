package river

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func TestJobDeleteManyParams_filtersEmpty(t *testing.T) {
	t.Parallel()

	require.True(t, NewJobDeleteManyParams().filtersEmpty())

	require.False(t, NewJobDeleteManyParams().IDs(123).filtersEmpty())
	require.False(t, NewJobDeleteManyParams().Kinds("kind").filtersEmpty())
	require.False(t, NewJobDeleteManyParams().Priorities(1).filtersEmpty())
	require.False(t, NewJobDeleteManyParams().Queues("queues").filtersEmpty())
	require.False(t, NewJobDeleteManyParams().States(rivertype.JobStateAvailable).filtersEmpty())
	require.False(t, NewJobDeleteManyParams().Where("true").filtersEmpty())
}

func TestJobDeleteManyParams_UnsafeAll(t *testing.T) {
	t.Parallel()

	NewJobDeleteManyParams().UnsafeAll()

	require.PanicsWithValue(t, "UnsafeAll no longer meaningful with non-default filters applied", func() {
		NewJobDeleteManyParams().IDs(123).UnsafeAll()
	})

	require.PanicsWithValue(t, "UnsafeAll no longer meaningful with non-default filters applied", func() {
		NewJobDeleteManyParams().Where("true").UnsafeAll()
	})
}

func TestJobDeleteManyParams_toDBParams(t *testing.T) {
	t.Parallel()

	for _, state := range []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()

			params := NewJobDeleteManyParams().States(state).toDBParams()
			require.Empty(t, params.Where)
			driverParams, err := dblist.JobMakeDriverParams(context.Background(), params, riverpgxv5.New(nil))
			require.NoError(t, err)
			require.Equal(t, "state = any(@state)", driverParams.WhereClause)
			require.Equal(t, []string{string(state)}, driverParams.NamedArgs["state"])
		})
	}
}

func TestJobDeleteManyParams_Where(t *testing.T) {
	t.Parallel()

	t.Run("DoesNotModifyReceiver", func(t *testing.T) {
		t.Parallel()

		params := NewJobDeleteManyParams().Where("true")
		_ = params.Where("false")
		require.Len(t, params.where, 1)
	})

	t.Run("MergesNamedArgsWithoutMutatingThem", func(t *testing.T) {
		t.Parallel()

		var (
			namedArgs1 = NamedArgs{"a": 1, "b": 2}
			namedArgs2 = NamedArgs{"b": 3}
		)

		params := NewJobDeleteManyParams().Where("a = @a AND b = @b", namedArgs1, namedArgs2)
		require.Equal(t, map[string]any{"a": 1, "b": 3}, params.where[0].NamedArgs)
		require.Equal(t, NamedArgs{"a": 1, "b": 2}, namedArgs1)
		require.Equal(t, NamedArgs{"b": 3}, namedArgs2)
	})

	t.Run("WrapsPredicateInParentheses", func(t *testing.T) {
		t.Parallel()

		params := NewJobDeleteManyParams().
			Kinds("kind").
			Where("priority = @priority1 OR priority = @priority2", NamedArgs{"priority1": 1, "priority2": 2}).
			Where("queue = @queue -- trailing comment", NamedArgs{"queue": "default"}).
			toDBParams()

		driverParams, err := dblist.JobMakeDriverParams(context.Background(), params, riverpgxv5.New(nil))
		require.NoError(t, err)
		require.Equal(t, "kind = any(@kind)\n  AND (priority = @priority1 OR priority = @priority2\n)\n  AND (queue = @queue -- trailing comment\n)", driverParams.WhereClause)
		require.Equal(t, 1, driverParams.NamedArgs["priority1"])
		require.Equal(t, 2, driverParams.NamedArgs["priority2"])
		require.Equal(t, "default", driverParams.NamedArgs["queue"])
	})
}
