package river

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
)

func TestMetadataSet(t *testing.T) {
	t.Parallel()

	t.Run("RejectsNonJSONValue", func(t *testing.T) {
		t.Parallel()

		metadataUpdates := map[string]any{}
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		require.EqualError(t, MetadataSet(ctx, "key", make(chan int)), "json: unsupported type: chan int")
		require.Empty(t, metadataUpdates)
	})

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

		encoded, err := json.Marshal(metadataUpdates)
		require.NoError(t, err)
		require.JSONEq(t, `{"key":"value"}`, string(encoded))
	})

	t.Run("SnapshotsValue", func(t *testing.T) {
		t.Parallel()

		metadataUpdates := map[string]any{}
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

		value := map[string]int{"count": 1}
		require.NoError(t, MetadataSet(ctx, "key", value))

		value["count"] = 2

		encoded, err := json.Marshal(metadataUpdates)
		require.NoError(t, err)
		require.JSONEq(t, `{"key":{"count":1}}`, string(encoded))
	})
}
