package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestJobUniqueOpts_validate(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&UniqueOpts{}).validate())
	require.NoError(t, (&UniqueOpts{
		ByArgs:   true,
		ByPeriod: 1 * time.Second,
		ByQueue:  true,
		ByState:  []rivertype.JobState{rivertype.JobStateAvailable},
	}).validate())

	require.EqualError(t, (&UniqueOpts{ByPeriod: 1 * time.Millisecond}).validate(), "JobUniqueOpts.ByPeriod should not be less than 1 second")
	require.EqualError(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobState("invalid")}}).validate(), `JobUniqueOpts.ByState contains invalid state "invalid"`)
}
