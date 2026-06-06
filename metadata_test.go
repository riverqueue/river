package river

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
)

func TestMetadataSet(t *testing.T) {
	t.Parallel()

	t.Run("RejectsReservedPrefix", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, map[string]any{})

		err := MetadataSet(ctx, "river:reserved", "value")
		require.EqualError(t, err, "MetadataSet cannot be used with keys prefixed with `river:`")
	})

	t.Run("RequiresWorkContext", func(t *testing.T) {
		t.Parallel()

		err := MetadataSet(context.Background(), "key", "value")
		require.EqualError(t, err, "MetadataSet must be called within a worker, worker middleware, or work hook")
	})

	t.Run("SetsValueOnWorkContext", func(t *testing.T) {
		t.Parallel()

		metadataUpdates := map[string]any{}
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		err := MetadataSet(ctx, "key", "value")
		require.NoError(t, err)
		require.Equal(t, "value", metadataUpdates["key"])
	})
}
