//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// Each response is a complete observation. json.Unmarshal alone merges maps
// into previous responses and can make deleted metadata appear to survive.
func decodeAdapterResult(encoded []byte, result any) error {
	value := reflect.ValueOf(result)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("adapter result must be a non-nil pointer, got %T", result)
	}
	fresh := reflect.New(value.Elem().Type())
	if err := json.Unmarshal(encoded, fresh.Interface()); err != nil {
		return err
	}
	value.Elem().Set(fresh.Elem())
	return nil
}

func TestDecodeAdapterResult(t *testing.T) {
	t.Parallel()

	var job normalizedJob
	require.NoError(t, decodeAdapterResult([]byte(`{"metadata":{"output":1,"nested":{"stale":true}}}`), &job))
	require.NoError(t, decodeAdapterResult([]byte(`{"metadata":{"nested":{"current":true}}}`), &job))
	require.Equal(t, map[string]any{"nested": map[string]any{"current": true}}, job.Metadata)
	require.NoError(t, decodeAdapterResult([]byte(`null`), &job))
	require.Nil(t, job.Metadata)
}
