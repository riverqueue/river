package dbtype

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
	t.Parallel()

	t.Run("ScanCopiesDriverBuffer", func(t *testing.T) {
		t.Parallel()

		buffer := []byte(`{"value":1}`)
		var value JSON
		require.NoError(t, value.Scan(buffer))
		buffer[9] = '2'
		require.Equal(t, JSON(`{"value":1}`), value)
	})

	t.Run("ScanRejectsUnexpectedTypes", func(t *testing.T) {
		t.Parallel()

		var value JSON
		require.ErrorContains(t, value.Scan(42), "cannot scan int into JSON")
	})

	t.Run("ScanSQLNull", func(t *testing.T) {
		t.Parallel()

		value := JSON(`null`)
		require.NoError(t, value.Scan(nil))
		require.Nil(t, value)
	})

	t.Run("ScanText", func(t *testing.T) {
		t.Parallel()

		var value JSON
		require.NoError(t, value.Scan(`{"value":1}`))
		require.Equal(t, JSON(`{"value":1}`), value)
	})

	t.Run("ValueDistinguishesSQLNullAndJSONNull", func(t *testing.T) {
		t.Parallel()

		value, err := JSON(nil).Value()
		require.NoError(t, err)
		require.Nil(t, value)
		value, err = JSON(`null`).Value()
		require.NoError(t, err)
		require.Equal(t, "null", value)
	})
}
