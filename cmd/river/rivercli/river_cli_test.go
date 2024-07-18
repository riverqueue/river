package rivercli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivermigrate"
)

func TestMigrationComment(t *testing.T) {
	t.Parallel()

	require.Equal(t, "-- River migration 001 [down]", migrationComment(1, rivermigrate.DirectionDown))
	require.Equal(t, "-- River migration 002 [up]", migrationComment(2, rivermigrate.DirectionUp))
}

func TestRoundDuration(t *testing.T) {
	t.Parallel()

	mustParseDuration := func(s string) time.Duration {
		d, err := time.ParseDuration(s)
		require.NoError(t, err)
		return d
	}

	require.Equal(t, "1.33µs", roundDuration(mustParseDuration("1.332µs")).String())
	require.Equal(t, "765.62µs", roundDuration(mustParseDuration("765.625µs")).String())
	require.Equal(t, "4.42ms", roundDuration(mustParseDuration("4.422125ms")).String())
	require.Equal(t, "13.28ms", roundDuration(mustParseDuration("13.280834ms")).String())
	require.Equal(t, "234.91ms", roundDuration(mustParseDuration("234.91075ms")).String())
	require.Equal(t, "3.93s", roundDuration(mustParseDuration("3.937042s")).String())
	require.Equal(t, "34.04s", roundDuration(mustParseDuration("34.042234s")).String())
	require.Equal(t, "2m34.04s", roundDuration(mustParseDuration("2m34.042234s")).String())
}
