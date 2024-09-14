package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestTagRE(t *testing.T) {
	t.Parallel()

	require.Regexp(t, tagRE, "aaa")
	require.Regexp(t, tagRE, "_aaa")
	require.Regexp(t, tagRE, "aaa_")
	require.Regexp(t, tagRE, "777")
	require.Regexp(t, tagRE, "my-tag")
	require.Regexp(t, tagRE, "my_tag")
	require.Regexp(t, tagRE, "my-longer-tag")
	require.Regexp(t, tagRE, "my_longer_tag")
	require.Regexp(t, tagRE, "My_Capitalized_Tag")
	require.Regexp(t, tagRE, "ALL_CAPS")
	require.Regexp(t, tagRE, "1_2_3")

	require.NotRegexp(t, tagRE, "a")
	require.NotRegexp(t, tagRE, "aa")
	require.NotRegexp(t, tagRE, "-aaa")
	require.NotRegexp(t, tagRE, "aaa-")
	require.NotRegexp(t, tagRE, "special@characters$banned")
	require.NotRegexp(t, tagRE, "commas,never,allowed")
}

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

func TestJobUniqueOpts_isV1(t *testing.T) {
	t.Parallel()

	// Test when ByState is empty
	require.False(t, (&UniqueOpts{}).isV1())

	// Test when ByState contains none of the required V3 states
	require.True(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateCompleted}}).isV1())

	// Test when ByState contains some but not all required V3 states
	require.True(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStatePending}}).isV1())
	require.True(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateScheduled}}).isV1())
	require.True(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateAvailable}}).isV1())
	require.True(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateRunning}}).isV1())

	// Test when ByState contains all required V3 states
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStatePending,
		rivertype.JobStateScheduled,
		rivertype.JobStateAvailable,
		rivertype.JobStateRunning,
	}}).isV1())

	// Test when ByState contains more than the required V3 states
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStatePending,
		rivertype.JobStateScheduled,
		rivertype.JobStateAvailable,
		rivertype.JobStateRunning,
		rivertype.JobStateCompleted,
	}}).isV1())
}
