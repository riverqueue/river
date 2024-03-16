package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivermigrate"
)

func TestMigrationComment(t *testing.T) {
	t.Parallel()

	require.Equal(t, "-- River migration 001 [down]", migrationComment(1, rivermigrate.DirectionDown))
	require.Equal(t, "-- River migration 002 [up]", migrationComment(2, rivermigrate.DirectionUp))
}
