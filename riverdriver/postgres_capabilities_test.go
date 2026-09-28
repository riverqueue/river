package riverdriver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPostgresCapabilities(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		expectedListenNotify bool
		expectedUniqueMode   UniqueInsertMode
		name                 string
		product              string
		version              int32
		ybListenNotify       bool
	}{
		{expectedListenNotify: true, expectedUniqueMode: UniqueInsertModeXmax, name: "Postgres15", product: "PostgreSQL 15.12", version: 150_012},
		{expectedListenNotify: true, expectedUniqueMode: UniqueInsertModeReturningOld, name: "Postgres18", product: "PostgreSQL 18.0", version: 180_000},
		{expectedUniqueMode: UniqueInsertModeMetadataNonce, name: "YugabyteDisabled", product: "PostgreSQL 15.12-YB-2025.2.3.0-b1", version: 150_012},
		{expectedListenNotify: true, expectedUniqueMode: UniqueInsertModeMetadataNonce, name: "YugabyteEnabled", product: "PostgreSQL 15.12-YB-2025.2.3.0-b1", version: 150_012, ybListenNotify: true},
		{expectedUniqueMode: UniqueInsertModeMetadataNonce, name: "YugabyteOld", product: "PostgreSQL 15.12-YB-2025.2.1.0-b1", version: 150_012},
		{expectedUniqueMode: UniqueInsertModeMetadataNonce, name: "YugabyteProductName", product: "YugabyteDB", version: 150_012},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			capabilities := NewPostgresCapabilities(testCase.product, testCase.version, testCase.ybListenNotify)
			require.Equal(t, testCase.expectedListenNotify, capabilities.SupportsListenNotify)
			require.Equal(t, testCase.expectedUniqueMode, capabilities.UniqueInsertMode)
		})
	}
}
