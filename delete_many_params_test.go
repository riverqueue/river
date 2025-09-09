package river

import (
	"testing"

	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
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
