//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func verifyUniqueKeyGoldens(t *testing.T, repositoryRoot string, adapters ...*adapter) {
	t.Helper()

	var fixture struct {
		Cases []json.RawMessage `json:"cases"`
	}
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/fixtures/unique_keys.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &fixture))
	require.NotEmpty(t, fixture.Cases)

	for _, encodedCase := range fixture.Cases {
		var expected struct {
			ExpectedSHA256    string `json:"expected_sha256"`
			ExpectedStateMask int    `json:"expected_state_mask"`
			Name              string `json:"name"`
		}
		require.NoError(t, json.Unmarshal(encodedCase, &expected))
		for _, adapter := range adapters {
			var actual struct {
				SHA256    string `json:"sha256"`
				StateMask int    `json:"state_mask"`
			}
			adapter.call(t, "unique_key", encodedCase, &actual)
			require.Equal(t, expected.ExpectedSHA256, actual.SHA256,
				"%s adapter fixture %s", adapter.name, expected.Name)
			require.Equal(t, expected.ExpectedStateMask, actual.StateMask,
				"%s adapter fixture %s", adapter.name, expected.Name)
		}
	}
}
