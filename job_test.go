package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestJobUniqueOpts_isEmpty(t *testing.T) {
	t.Parallel()

	require.True(t, (&UniqueOpts{}).isEmpty())
	require.False(t, (&UniqueOpts{ByArgs: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByPeriod: 1 * time.Nanosecond}).isEmpty())
	require.False(t, (&UniqueOpts{ByQueue: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{JobStateAvailable}}).isEmpty())
}

func TestJobUniqueOpts_validate(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&UniqueOpts{}).validate())
	require.NoError(t, (&UniqueOpts{
		ByArgs:   true,
		ByPeriod: 1 * time.Second,
		ByQueue:  true,
		ByState:  []rivertype.JobState{JobStateAvailable},
	}).validate())

	require.EqualError(t, (&UniqueOpts{ByPeriod: 1 * time.Millisecond}).validate(), "JobUniqueOpts.ByPeriod should not be less than 1 second")
	require.EqualError(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobState("invalid")}}).validate(), `JobUniqueOpts.ByState contains invalid state "invalid"`)
}
