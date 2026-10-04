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
			ExpectedError     string `json:"expected_error"`
			ExpectedSHA256    string `json:"expected_sha256"`
			ExpectedStateMask int    `json:"expected_state_mask"`
			Name              string `json:"name"`
		}
		require.NoError(t, json.Unmarshal(encodedCase, &expected))
		for _, adapter := range adapters {
			if expected.ExpectedError != "" {
				adapter.requireCallError(t, "unique_key", encodedCase, expected.ExpectedError)
				continue
			}
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

// verifyUniqueSkipKeepsExistingKind has one implementation insert a job unique
// by args with `exclude_kind`, then gives it another kind out of band, which
// leaves its unique key shared with `conformance_echo` insertions of the same
// args. The other implementation inserts those args singly and in a batch.
// Both are skipped as duplicates, and both must return the existing job and
// leave it as it was rather than rewriting its kind to their own, which would
// hand it to the wrong worker.
func verifyUniqueSkipKeepsExistingKind(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const existingKind = "conformance_unique_other_kind"
	for _, pair := range []struct {
		first   *adapter
		skipper *adapter
	}{
		{first: goAdapter, skipper: candidateAdapter},
		{first: candidateAdapter, skipper: goAdapter},
	} {
		pair.first.call(t, "reset", map[string]any{}, nil)
		job := map[string]any{
			"message": "unique skip keeps kind " + pair.first.name,
			"opts": map[string]any{
				"unique": map[string]any{"by_args": true, "exclude_kind": true},
			},
		}
		var existing normalizedJob
		pair.first.call(t, "insert", job, &existing)
		require.Equal(t, "conformance_echo", existing.Kind)
		pair.first.call(t, "raw_set_kind", map[string]any{"id": existing.ID, "kind": existingKind}, &existing)
		require.Equal(t, existingKind, existing.Kind)

		var single normalizedJob
		pair.skipper.call(t, "insert", job, &single)
		require.Equal(t, existing, single, "%s insert", pair.skipper.name)

		var batch struct {
			Results []normalizedInsertResult `json:"results"`
		}
		pair.skipper.call(t, "insert_many", map[string]any{"jobs": []map[string]any{job}}, &batch)
		require.Len(t, batch.Results, 1)
		require.True(t, batch.Results[0].UniqueSkippedAsDuplicate, "%s insert_many", pair.skipper.name)
		require.Equal(t, existing, batch.Results[0].Job, "%s insert_many", pair.skipper.name)

		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.first.call(t, "list", map[string]any{}, &listed)
		require.Equal(t, []normalizedJob{existing}, listed.Jobs)
	}
}
