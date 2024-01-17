package riverdatabasesql

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"weavelab.xyz/river/riverdriver"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		dbPool := &sql.DB{}
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
	require.ErrorIs(t, interpretError(sql.ErrNoRows), riverdriver.ErrNoRows)
	require.NoError(t, interpretError(nil))
}
