package riverpgxv5

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		dbPool := &pgxpool.Pool{}
		driver := New(dbPool)
		require.Equal(t, dbPool, driver.dbPool)
	})

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		driver := New(nil)
		require.Nil(t, driver.dbPool)
	})
}
