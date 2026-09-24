package harness_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// conformanceTestsStarted counts conformance tests that began executing so a
// required run can detect a -run pattern that matched nothing.
var conformanceTestsStarted atomic.Int32 //nolint:gochecknoglobals // shared with TestMain

func TestCompatibilityArtifacts(t *testing.T) {
	t.Parallel()

	conformanceTestsStarted.Add(1)

	root := compatibilityRepositoryRoot(t)
	readJSON := func(t *testing.T, path string, target any) {
		t.Helper()

		contents, err := os.ReadFile(filepath.Join(root, path))
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(contents, target))
	}

	t.Run("CapabilitiesDecided", func(t *testing.T) {
		t.Parallel()

		type implementation struct {
			Package  string `json:"package"`
			Registry string `json:"registry"`
			Version  string `json:"version"`
		}
		var manifest struct {
			Capabilities        map[string]string         `json:"capabilities"`
			CapabilityDecisions map[string]string         `json:"capability_decisions"`
			Implementations     map[string]implementation `json:"implementations"`
		}
		readJSON(t, "conformance/manifest.json", &manifest)
		require.NotEmpty(t, manifest.Capabilities)
		for capability, status := range manifest.Capabilities {
			require.Contains(t, []string{"complete", "in_progress", "not_applicable", "planned"}, status,
				"capability %s", capability)
			if status == "complete" {
				require.NotContains(t, manifest.CapabilityDecisions, capability,
					"complete capability %s needs no applicability decision", capability)
			} else {
				require.NotEmpty(t, manifest.CapabilityDecisions[capability],
					"capability %s is %s and must record its applicability decision", capability, status)
			}
		}
		for capability := range manifest.CapabilityDecisions {
			require.Contains(t, manifest.Capabilities, capability, "decision for unknown capability %s", capability)
		}

		// Go is the reference; every other entry is a candidate, and the
		// set of candidates is open.
		require.Contains(t, manifest.Implementations, "go")
		for name, implementation := range manifest.Implementations {
			require.NotEmpty(t, implementation.Package, "implementation %s package", name)
			require.NotEmpty(t, implementation.Registry, "implementation %s registry", name)
			require.NotEmpty(t, implementation.Version, "implementation %s version", name)
		}
	})

	t.Run("AdapterContractComplete", func(t *testing.T) {
		t.Parallel()

		var manifest struct {
			Capabilities     map[string]string `json:"capabilities"`
			ProtocolRevision int               `json:"protocol_revision"`
		}
		readJSON(t, "conformance/manifest.json", &manifest)
		var contract struct {
			AdapterVersion int `json:"adapter_version"`
			Methods        []struct {
				Capability  string `json:"capability"`
				Description string `json:"description"`
				Name        string `json:"name"`
			} `json:"methods"`
			ProtocolRevision int `json:"protocol_revision"`
		}
		readJSON(t, "conformance/adapter/contract.json", &contract)
		require.Positive(t, contract.AdapterVersion)
		require.Equal(t, manifest.ProtocolRevision, contract.ProtocolRevision)
		names := make([]string, 0, len(contract.Methods))
		for _, method := range contract.Methods {
			require.Contains(t, manifest.Capabilities, method.Capability, "method %s", method.Name)
			require.NotEmpty(t, method.Description, "method %s", method.Name)
			require.NotContains(t, names, method.Name)
			names = append(names, method.Name)
		}
		require.True(t, slices.IsSorted(names))
		require.Contains(t, names, "handshake")
	})

	t.Run("AdapterProfilesAreContractSubsets", func(t *testing.T) {
		t.Parallel()

		var contract struct {
			Methods []struct {
				Capability string `json:"capability"`
				Name       string `json:"name"`
			} `json:"methods"`
			ProtocolRevision int `json:"protocol_revision"`
		}
		readJSON(t, "conformance/adapter/contract.json", &contract)
		contractMethods := make(map[string]string, len(contract.Methods))
		for _, method := range contract.Methods {
			contractMethods[method.Name] = method.Capability
		}

		type profileArtifact struct {
			Backend          string   `json:"backend"`
			Capabilities     []string `json:"capabilities"`
			Extends          string   `json:"extends"`
			Methods          []string `json:"methods"`
			Name             string   `json:"name"`
			ProtocolRevision int      `json:"protocol_revision"`
		}
		profiles := make(map[string]profileArtifact)
		for _, path := range []string{
			"conformance/adapter/profiles/sqlite-runtime.json",
			"conformance/adapter/profiles/sqlite.json",
		} {
			var profile profileArtifact
			readJSON(t, path, &profile)
			profiles[profile.Name] = profile
			require.Equal(t, "sqlite", profile.Backend)
			require.NotEmpty(t, profile.Name)
			require.Equal(t, contract.ProtocolRevision, profile.ProtocolRevision)
			require.True(t, slices.IsSorted(profile.Capabilities))
			require.True(t, slices.IsSorted(profile.Methods))
			require.Contains(t, profile.Methods, "handshake")
			for _, method := range profile.Methods {
				capability, ok := contractMethods[method]
				require.True(t, ok, "profile method %q is absent from the full contract", method)
				require.Contains(t, profile.Capabilities, capability,
					"profile method %q requires capability %q", method, capability)
			}
		}
		for name, profile := range profiles {
			if profile.Extends == "" {
				continue
			}
			base, ok := profiles[profile.Extends]
			require.True(t, ok, "profile %q extends missing profile %q", name, profile.Extends)
			require.Subset(t, profile.Capabilities, base.Capabilities,
				"profile %q must retain all %q capabilities", name, profile.Extends)
			require.Subset(t, profile.Methods, base.Methods,
				"profile %q must retain all %q methods", name, profile.Extends)
		}
	})

	t.Run("CandidateDescriptorsValid", func(t *testing.T) {
		t.Parallel()

		var manifest struct {
			Implementations map[string]struct {
				Version string `json:"version"`
			} `json:"implementations"`
		}
		readJSON(t, "conformance/manifest.json", &manifest)
		paths, err := filepath.Glob(filepath.Join(root, "conformance/adapter/candidates/*.json"))
		require.NoError(t, err)
		require.NotEmpty(t, paths)
		for _, path := range paths {
			contents, err := os.ReadFile(path)
			require.NoError(t, err)
			descriptor := decodeDescriptor(t, contents)
			require.Equal(t, strings.TrimSuffix(filepath.Base(path), ".json"), descriptor.Implementation,
				"%s must be named after its implementation", path)
			require.NotEqual(t, "go", descriptor.Implementation, "Go is the reference, not a candidate")
			require.Contains(t, manifest.Implementations, descriptor.Implementation)
			require.Equal(t, manifest.Implementations[descriptor.Implementation].Version, descriptor.Version)
		}
	})

	t.Run("MigrationInventoryComplete", func(t *testing.T) {
		t.Parallel()

		type migrationInventory struct {
			Database string `json:"database"`
			Files    []struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"files"`
			Line string `json:"line"`
		}
		for path, database := range map[string]string{
			"conformance/migrations.json":        "postgres",
			"conformance/migrations-sqlite.json": "sqlite",
		} {
			var migrations migrationInventory
			readJSON(t, path, &migrations)
			require.Equal(t, database, migrations.Database)
			require.Equal(t, "main", migrations.Line)
			require.Len(t, migrations.Files, 14)
			for _, file := range migrations.Files {
				require.Len(t, file.SHA256, 64)
				require.True(t, strings.HasSuffix(file.Path, ".sql"))
			}
		}
	})

	t.Run("ScenariosUniqueAndSorted", func(t *testing.T) {
		t.Parallel()

		for _, inventory := range []struct {
			path    string
			profile string
		}{
			{path: "conformance/scenarios/core.json"},
			{path: "conformance/scenarios/sqlite-runtime.json", profile: "sqlite-runtime-v1"},
			{path: "conformance/scenarios/sqlite-storage.json", profile: "portable-storage-v1"},
		} {
			verifyScenarioInventory(t, root, inventory.path, inventory.profile)
		}
	})

	t.Run("SchemaReferencesResolve", func(t *testing.T) {
		t.Parallel()

		for _, path := range []string{
			"conformance/adapter/candidates/rust.json",
			"conformance/adapter/contract.json",
			"conformance/adapter/profiles/sqlite-runtime.json",
			"conformance/adapter/profiles/sqlite.json",
			"conformance/fixtures/maintenance_values.json",
			"conformance/fixtures/protocol_values.json",
			"conformance/fixtures/unique_keys.json",
			"conformance/manifest.json",
			"conformance/scenarios/core.json",
			"conformance/scenarios/sqlite-runtime.json",
			"conformance/scenarios/sqlite-storage.json",
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

func verifyScenarioInventory(t *testing.T, root, path, profile string) {
	t.Helper()

	var inventory struct {
		Scenarios []struct {
			Evidence []struct {
				Path   string `json:"path"`
				Symbol string `json:"symbol"`
			} `json:"evidence"`
			Name string `json:"name"`
			Tier string `json:"tier"`
		} `json:"scenarios"`
	}
	contents, err := os.ReadFile(filepath.Join(root, path))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &inventory))

	names := make([]string, 0, len(inventory.Scenarios))
	tiers := make(map[string]bool)
	for _, scenario := range inventory.Scenarios {
		require.NotContains(t, names, scenario.Name)
		binding, ok := scenarioRegistry[scenario.Name]
		require.True(t, ok, "scenario %q has no executable test binding", scenario.Name)
		require.Equal(t, profile, binding.profile, "scenario %q profile", scenario.Name)
		require.Equal(t, binding.tier, scenario.Tier, "scenario %q tier", scenario.Name)
		require.NotEmpty(t, scenario.Evidence, "scenario %q has no executable evidence", scenario.Name)
		for _, evidence := range scenario.Evidence {
			require.True(t, strings.HasPrefix(evidence.Path, "conformance/harness/"),
				"scenario %q evidence must point into the executable harness", scenario.Name)
			require.NotContains(t, evidence.Path, "..")
			evidenceContents, err := os.ReadFile(filepath.Join(root, evidence.Path))
			require.NoError(t, err, "scenario %q evidence path", scenario.Name)
			require.Contains(t, string(evidenceContents), "func "+evidence.Symbol+"(",
				"scenario %q evidence symbol", scenario.Name)
		}
		names = append(names, scenario.Name)
		tiers[scenario.Tier] = true
	}
	require.True(t, slices.IsSorted(names))
	registeredNames := make([]string, 0, len(scenarioRegistry))
	for name, binding := range scenarioRegistry {
		if binding.profile == profile {
			registeredNames = append(registeredNames, name)
		}
	}
	slices.Sort(registeredNames)
	require.Equal(t, registeredNames, names,
		"%s and the executable scenario registry must have identical IDs", path)
	if profile == "" {
		for _, tier := range []string{"chaos", "codec", "mixed", "performance", "runtime", "storage"} {
			require.True(t, tiers[tier], "missing scenario tier %s", tier)
		}
	}
}

func compatibilityRepositoryRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
}
