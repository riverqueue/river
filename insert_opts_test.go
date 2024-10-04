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

func TestUniqueOpts_validate(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&UniqueOpts{}).validate())
	require.NoError(t, (&UniqueOpts{
		ByArgs:   true,
		ByPeriod: 1 * time.Second,
		ByQueue:  true,
	}).validate())

	require.EqualError(t, (&UniqueOpts{ByPeriod: 1 * time.Millisecond}).validate(), "UniqueOpts.ByPeriod should not be less than 1 second")
	require.EqualError(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobState("invalid")}}).validate(), `UniqueOpts.ByState contains invalid state "invalid"`)

	requiredStates := []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStatePending,
		rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}

	for _, state := range requiredStates {
		// Test with each state individually removed from requiredStates to ensure
		// it's validated.

		// Create a copy of requiredStates without the current state
		var testStates []rivertype.JobState
		for _, s := range requiredStates {
			if s != state {
				testStates = append(testStates, s)
			}
		}

		// Test validation
		require.EqualError(t, (&UniqueOpts{ByState: testStates}).validate(), "UniqueOpts.ByState must contain all required states, missing: "+string(state))
	}

	// test with more than one required state missing:
	require.EqualError(t, (&UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStateScheduled,
	}}).validate(), "UniqueOpts.ByState must contain all required states, missing: pending, running")

	require.NoError(t, (&UniqueOpts{ByState: rivertype.JobStates()}).validate())
}
