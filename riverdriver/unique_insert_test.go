package riverdriver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniqueInsertMetadataIsDuplicate(t *testing.T) {
	t.Parallel()

	t.Run("DifferentNonce", func(t *testing.T) {
		t.Parallel()

		require.True(t, UniqueInsertMetadataIsDuplicate([]byte(`{"river:unique_nonce":"old"}`), "new"))
	})

	t.Run("InvalidMetadata", func(t *testing.T) {
		t.Parallel()

		require.True(t, UniqueInsertMetadataIsDuplicate([]byte(`{`), "nonce"))
	})

	t.Run("MatchingNonce", func(t *testing.T) {
		t.Parallel()

		require.False(t, UniqueInsertMetadataIsDuplicate([]byte(`{"river:unique_nonce":"nonce"}`), "nonce"))
	})

	t.Run("MissingNonce", func(t *testing.T) {
		t.Parallel()

		require.True(t, UniqueInsertMetadataIsDuplicate([]byte(`{"existing":123}`), "nonce"))
	})
}

func TestUniqueInsertMetadataWithNonce(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		metadata, err := UniqueInsertMetadataWithNonce(nil, "nonce")
		require.NoError(t, err)
		require.JSONEq(t, `{"river:unique_nonce":"nonce"}`, string(metadata))
	})

	t.Run("ExistingMetadata", func(t *testing.T) {
		t.Parallel()

		metadata, err := UniqueInsertMetadataWithNonce([]byte(`{"existing":123}`), "nonce")
		require.NoError(t, err)
		require.JSONEq(t, `{"existing":123,"river:unique_nonce":"nonce"}`, string(metadata))
	})

	t.Run("ExistingNonce", func(t *testing.T) {
		t.Parallel()

		metadata, err := UniqueInsertMetadataWithNonce([]byte(`{"river:unique_nonce":"old"}`), "new")
		require.NoError(t, err)
		require.JSONEq(t, `{"river:unique_nonce":"new"}`, string(metadata))
	})

	t.Run("InvalidMetadata", func(t *testing.T) {
		t.Parallel()

		_, err := UniqueInsertMetadataWithNonce([]byte(`{`), "nonce")
		require.ErrorContains(t, err, "error unmarshaling job metadata")
	})
}

func TestUniqueInsertModeFromProductAndVersion(t *testing.T) {
	t.Parallel()

	t.Run("PostgreSQL17", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, UniqueInsertModeXmax, UniqueInsertModeFromProductAndVersion("PostgreSQL 17.5", 170_005))
	})

	t.Run("PostgreSQL18", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, UniqueInsertModeReturningOld, UniqueInsertModeFromProductAndVersion("PostgreSQL 18.0", 180_000))
	})

	t.Run("YugabyteByName", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, UniqueInsertModeMetadataNonce, UniqueInsertModeFromProductAndVersion("YugabyteDB", 180_000))
	})

	t.Run("YugabytePostgreSQLVersion", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, UniqueInsertModeMetadataNonce, UniqueInsertModeFromProductAndVersion("PostgreSQL 15.2-YB-2.25.1.0-b0", 150_002))
	})
}

func TestUniqueInsertModeSQL(t *testing.T) {
	t.Parallel()

	t.Run("MetadataNonce", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "false", UniqueInsertModeMetadataNonce.SQL())
	})

	t.Run("ReturningOld", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "(OLD.id IS NOT NULL)", UniqueInsertModeReturningOld.SQL())
	})

	t.Run("Unknown", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "unique insert mode has not been detected", func() { UniqueInsertModeUnknown.SQL() })
	})

	t.Run("Xmax", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "(xmax != 0)", UniqueInsertModeXmax.SQL())
	})
}
