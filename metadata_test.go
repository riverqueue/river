package river

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
)

func TestMetadataSet(t *testing.T) {
	t.Parallel()

	t.Run("RejectsNonJSONValues", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name  string
			value any
		}{
			{name: "Channel", value: make(chan int)},
			{name: "InvalidRawMessage", value: json.RawMessage(`{"unfinished":`)},
			{name: "NaN", value: math.NaN()},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				metadataUpdates := map[string]any{"existing": "kept", "key": "previous"}
				ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)

				require.Error(t, MetadataSet(ctx, "key", tt.value))
				require.Equal(t, map[string]any{"existing": "kept", "key": "previous"}, metadataUpdates)
			})
		}
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
		value := map[string]any{"count": 1}

		require.NoError(t, MetadataSet(ctx, "key", value))
		value["count"] = math.NaN()
		encoded, err := json.Marshal(metadataUpdates)
		require.NoError(t, err)
		require.JSONEq(t, `{"key":{"count":1}}`, string(encoded))
	})
}
