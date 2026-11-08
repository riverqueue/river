package rivercli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseMySQLURL(t *testing.T) {
	t.Parallel()

	t.Run("ConnectionOptions", func(t *testing.T) {
		t.Parallel()

		config, err := parseMySQLURL("mysql://worker:p%40ss%3Fword@[::1]:3307/river%20test?tls=preferred&timeout=5s")
		require.NoError(t, err)
		require.Equal(t, "[::1]:3307", config.Addr)
		require.Equal(t, "river test", config.DBName)
		require.Equal(t, "worker", config.User)
		require.Equal(t, "p@ss?word", config.Passwd)
		require.Equal(t, "preferred", config.TLSConfig)
		require.Equal(t, 5*time.Second, config.Timeout)
	})

	t.Run("InvalidConnectionOption", func(t *testing.T) {
		t.Parallel()

		_, err := parseMySQLURL("mysql://localhost/river?timeout=invalid")
		require.ErrorContains(t, err, "MySQL connection options")
	})

	t.Run("RequiredOptions", func(t *testing.T) {
		t.Parallel()

		config, err := parseMySQLURL("mysql://localhost/river?parseTime=false&multiStatements=false&loc=America%2FChicago")
		require.NoError(t, err)
		require.Equal(t, "localhost:3306", config.Addr)
		require.Same(t, time.UTC, config.Loc)
		require.True(t, config.MultiStatements)
		require.True(t, config.ParseTime)
	})
}
