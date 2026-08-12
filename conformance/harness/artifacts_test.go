package harness_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompatibilityArtifacts(t *testing.T) {
	t.Parallel()

	root := compatibilityRepositoryRoot(t)
	readJSON := func(t *testing.T, path string, target any) {
		t.Helper()

		contents, err := os.ReadFile(filepath.Join(root, path))
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(contents, target))
	}

	t.Run("CapabilitiesComplete", func(t *testing.T) {
		t.Parallel()

		var manifest struct {
			Capabilities map[string]string `json:"capabilities"`
			Rust struct {
				Version string `json:"version"`
			} `json:"rust"`
		}
		readJSON(t, "conformance/manifest.json", &manifest)
		require.NotEmpty(t, manifest.Capabilities)
		for capability, status := range manifest.Capabilities {
			require.Equal(t, "complete", status, "capability %s", capability)
		}

		cargoManifest, err := os.ReadFile(filepath.Join(root, "rust/Cargo.toml"))
		require.NoError(t, err)
		require.Contains(t, string(cargoManifest), "version = \""+manifest.Rust.Version+"\"")
	})

	t.Run("MigrationInventoryComplete", func(t *testing.T) {
		t.Parallel()

		var migrations struct {
			Files []struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"files"`
			Line string `json:"line"`
		}
		readJSON(t, "conformance/migrations.json", &migrations)
		require.Equal(t, "main", migrations.Line)
		require.Len(t, migrations.Files, 14)
		for _, file := range migrations.Files {
			require.Len(t, file.SHA256, 64)
			require.True(t, strings.HasSuffix(file.Path, ".sql"))
		}
	})

	t.Run("ScenariosUniqueAndSorted", func(t *testing.T) {
		t.Parallel()

		var scenarios struct {
			Scenarios []struct {
				Name string `json:"name"`
				Tier string `json:"tier"`
			} `json:"scenarios"`
		}
		readJSON(t, "conformance/scenarios/core.json", &scenarios)
		names := make([]string, 0, len(scenarios.Scenarios))
		tiers := make(map[string]bool)
		for _, scenario := range scenarios.Scenarios {
			require.NotContains(t, names, scenario.Name)
			names = append(names, scenario.Name)
			tiers[scenario.Tier] = true
		}
		require.True(t, slices.IsSorted(names))
		for _, tier := range []string{"chaos", "codec", "mixed", "performance", "runtime", "storage"} {
			require.True(t, tiers[tier], "missing scenario tier %s", tier)
		}
	})

	t.Run("SchemaReferencesResolve", func(t *testing.T) {
		t.Parallel()

		for _, path := range []string{
			"conformance/fixtures/protocol_values.json",
			"conformance/fixtures/unique_keys.json",
			"conformance/manifest.json",
			"conformance/scenarios/core.json",
		} {
			contents, err := os.ReadFile(filepath.Join(root, path))
			require.NoError(t, err)
			var artifact struct {
				Schema string `json:"$schema"`
			}
			require.NoError(t, json.Unmarshal(contents, &artifact))
			require.NotEmpty(t, artifact.Schema, "%s must declare a schema", path)

			schemaPath := filepath.Clean(filepath.Join(filepath.Dir(path), artifact.Schema))
			schemaContents, err := os.ReadFile(filepath.Join(root, schemaPath))
			require.NoError(t, err, "schema for %s", path)
			var schema any
			require.NoError(t, json.Unmarshal(schemaContents, &schema), "schema for %s", path)
		}
	})
}

func compatibilityRepositoryRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
}
