package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/rivertype"
)

func TestUniqueOpts_isEmpty(t *testing.T) {
	t.Parallel()

	require.True(t, (&UniqueOpts{}).isEmpty())
	require.False(t, (&UniqueOpts{ByArgs: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByPeriod: 1 * time.Nanosecond}).isEmpty())
	require.False(t, (&UniqueOpts{ByQueue: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateAvailable}}).isEmpty())

	require.False(t, (&UniqueOpts{ExcludeKind: true}).isEmpty())

	states := []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStatePending,
		rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}
	for _, byArgs := range []bool{false, true} {
		for _, byQueue := range []bool{false, true} {
			for _, byPeriod := range []time.Duration{0, 10 * time.Second} {
				for _, byState := range [][]rivertype.JobState{nil, states} {
					for _, excludeKind := range []bool{false, true} {
						opts := UniqueOpts{
							ByArgs:      byArgs,
							ByPeriod:    byPeriod,
							ByQueue:     byQueue,
							ByState:     byState,
							ExcludeKind: excludeKind,
						}
						internalOpts := (*dbunique.UniqueOpts)(&opts)

						require.Equal(t, internalOpts.IsEmpty(), opts.isEmpty(),
							"isEmpty and internal IsEmpty should agree for opts %+v", opts)
					}
				}
			}
		}
	}
}
