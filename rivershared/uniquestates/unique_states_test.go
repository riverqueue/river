package uniquestates

import (
	"testing"

	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

func TestUniqueStatesToBitmask(t *testing.T) {
	t.Parallel()

	bitmask := UniqueStatesToBitmask(rivertype.UniqueOptsByStateDefault())
	require.Equal(t, byte(0b11110101), bitmask, "Default unique states should be all set except cancelled and discarded")

	for state, position := range jobStateBitPositions {
		bitmask = UniqueStatesToBitmask([]rivertype.JobState{state})
		// Bit shifting uses postgres bit numbering with MSB on the right, so we
		// need to flip the position when shifting manually:
		require.Equal(t, byte(1<<(7-position)), bitmask, "Bitmask should be set for single state %s", state)
	}
}
