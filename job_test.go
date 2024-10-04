package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestUniqueOpts_isEmpty(t *testing.T) {
	t.Parallel()

	require.True(t, (&UniqueOpts{}).isEmpty())
	require.False(t, (&UniqueOpts{ByArgs: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByPeriod: 1 * time.Nanosecond}).isEmpty())
	require.False(t, (&UniqueOpts{ByQueue: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateAvailable}}).isEmpty())
}
