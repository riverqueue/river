package riverpgxv5

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"weavelab.xyz/river/riverdriver"
)

// Verify interface compliance.
var _ riverdriver.Driver[pgx.Tx] = New(nil)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		dbPool := &pgxpool.Pool{}
		driver := New(dbPool)
		require.Equal(t, dbPool, driver.dbPool)
	})

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		driver := New(nil)
		require.Nil(t, driver.dbPool)
	})
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(pgx.ErrNoRows), riverdriver.ErrNoRows)
	require.NoError(t, interpretError(nil))
}
