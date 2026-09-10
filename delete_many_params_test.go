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
}

func TestJobDeleteManyParams_UnsafeAll(t *testing.T) {
	t.Parallel()

	NewJobDeleteManyParams().UnsafeAll()

	require.PanicsWithValue(t, "UnsafeAll no longer meaningful with non-default filters applied", func() {
		NewJobDeleteManyParams().IDs(123).UnsafeAll()
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
