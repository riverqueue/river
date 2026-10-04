//go:build riverconformance

package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Profile names a candidate descriptor may declare.
const (
	profileInsertOnly      = "insert-only-v1"
	profilePortableStorage = "portable-storage-v1"
	profilePostgresFull    = "postgres-full-v1"
	profileSQLiteRuntime   = "sqlite-runtime-v1"
)

// defaultCandidateProfiles are assumed for descriptors that do not declare
// profiles, which keeps descriptors written before profiles existed valid.
var defaultCandidateProfiles = []string{profilePortableStorage, profilePostgresFull, profileSQLiteRuntime} //nolint:gochecknoglobals // descriptor default

// performanceBound returns the candidate's bound for a benchmark mode.
func (spec adapterSpec) performanceBound(mode string) performanceBound {
	if bound, ok := spec.Performance[mode]; ok {
		return bound
	}
	return defaultPerformanceBounds[mode]
}

// servesProfile reports whether the candidate declares a conformance profile.
func (spec adapterSpec) servesProfile(profile string) bool {
	if spec.Profiles == nil {
		return slices.Contains(defaultCandidateProfiles, profile)
	}
	return slices.Contains(spec.Profiles, profile)
}

// requireProfile skips an owner whose profile the candidate does not
// declare, or fails when RIVER_CONFORMANCE_REQUIRED=1 selected it anyway.
func (spec adapterSpec) requireProfile(t *testing.T, profile string) {
	t.Helper()

	if spec.servesProfile(profile) {
		return
	}
	if conformanceRequired() {
		t.Fatalf("%s candidate does not declare the %s profile this test requires", spec.Implementation, profile)
	}
	t.Skipf("%s candidate does not declare the %s profile", spec.Implementation, profile)
}

// supportsStartOption reports whether the candidate honors an optional
// `start` tuning parameter.
func (spec adapterSpec) supportsStartOption(option string) bool {
	return slices.Contains(spec.StartOptions, option)
}

// withStartOptions copies params and adds the optional tuning parameters the
// candidate declares it honors.
func (spec adapterSpec) withStartOptions(params map[string]any, options map[string]any) map[string]any {
	merged := make(map[string]any, len(params)+len(options))
	maps.Copy(merged, params)
	for key, value := range options {
		if spec.supportsStartOption(key) {
			merged[key] = value
		}
	}
	return merged
}

// referenceSpec describes the Go reference implementation. The reference
// honors no optional start tuning parameters because Go does not expose
// them as configuration.
func referenceSpec() adapterSpec {
	return adapterSpec{ApplicationName: referenceApplicationName, Implementation: "go"}
}

// conformanceCandidateSpec loads the candidate descriptor from
// RIVER_CONFORMANCE_CANDIDATE (inline JSON) or RIVER_CONFORMANCE_CANDIDATE_FILE,
// defaulting to the checked Rust descriptor, and builds it once.
func conformanceCandidateSpec(t *testing.T, root string, release bool) adapterSpec {
	t.Helper()

	specs := loadDescriptors(t, root, "RIVER_CONFORMANCE_CANDIDATE", "RIVER_CONFORMANCE_CANDIDATE_FILE")
	require.Len(t, specs, 1, "exactly one candidate descriptor is required")
	return prepareCandidate(t, root, specs[0], release)
}

// conformancePeerSpecs loads additional candidates for multi-engine tiers
// from RIVER_CONFORMANCE_PEER (an inline descriptor object or array) or
// RIVER_CONFORMANCE_PEER_FILE (one or more descriptor paths separated by the
// platform's path list separator), defaulting to the checked Rust descriptor.
func conformancePeerSpecs(t *testing.T, root string, release bool) []adapterSpec {
	t.Helper()

	specs := loadDescriptors(t, root, "RIVER_CONFORMANCE_PEER", "RIVER_CONFORMANCE_PEER_FILE")
	for index := range specs {
		specs[index] = prepareCandidate(t, root, specs[index], release)
	}
	return specs
}

func loadDescriptors(t *testing.T, root, inlineVariable, fileVariable string) []adapterSpec {
	t.Helper()

	encoded := os.Getenv(inlineVariable)
	paths := os.Getenv(fileVariable)
	require.False(t, encoded != "" && paths != "", "set only one of %s or %s", inlineVariable, fileVariable)

	var documents [][]byte
	switch {
	case encoded != "":
		trimmed := bytes.TrimSpace([]byte(encoded))
		if len(trimmed) > 0 && trimmed[0] == '[' {
			var raw []json.RawMessage
			require.NoError(t, json.Unmarshal(trimmed, &raw), "%s must be a descriptor object or array", inlineVariable)
			for _, document := range raw {
				documents = append(documents, document)
			}
		} else {
			documents = append(documents, trimmed)
		}
	default:
		if paths == "" {
			paths = "conformance/adapter/candidates/rust.json"
		}
		for _, descriptorPath := range filepath.SplitList(paths) {
			if !filepath.IsAbs(descriptorPath) {
				descriptorPath = filepath.Join(root, descriptorPath)
			}
			//nolint:gosec // The caller explicitly selects a local candidate descriptor.
			document, err := os.ReadFile(descriptorPath)
			require.NoError(t, err)
			documents = append(documents, document)
		}
	}
	require.NotEmpty(t, documents, "%s selects no descriptor", fileVariable)

	specs := make([]adapterSpec, 0, len(documents))
	for _, document := range documents {
		specs = append(specs, decodeDescriptor(t, document))
	}
	return specs
}

// candidateBuilds records build commands that already ran in this test
// process, keyed by their arguments.
var candidateBuilds sync.Map //nolint:gochecknoglobals // one build per process

// prepareCandidate selects the debug or release commands, runs the
// descriptor's build command once per process, and requires the restart
// command's executable to exist. Restart scenarios run the executable
// directly, so it must be the artifact the build just produced rather than
// whatever an earlier build left behind.
func prepareCandidate(t *testing.T, root string, spec adapterSpec, release bool) adapterSpec {
	t.Helper()

	build := spec.BuildCommand
	if release {
		if len(spec.ReleaseCommand) > 0 {
			spec.Command = slices.Clone(spec.ReleaseCommand)
			spec.RestartCommand = slices.Clone(spec.ReleaseCommand)
		}
		if len(spec.ReleaseBuildCommand) > 0 {
			build = spec.ReleaseBuildCommand
		}
	}
	if len(spec.RestartCommand) == 0 {
		spec.RestartCommand = slices.Clone(spec.Command)
	}
	if len(build) > 0 {
		key := strings.Join(build, "\x00")
		once, _ := candidateBuilds.LoadOrStore(key, &sync.Once{})
		once.(*sync.Once).Do(func() { //nolint:forcetypeassert // The map only stores *sync.Once under build keys.
			//nolint:gosec // The descriptor explicitly names its build command.
			command := exec.CommandContext(context.Background(), build[0], build[1:]...)
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				candidateBuilds.Store(key+"\x00failed", fmt.Sprintf("%v\n%s", err, output))
			}
		})
		if failure, failed := candidateBuilds.Load(key + "\x00failed"); failed {
			t.Fatalf("%s candidate build %v failed:\n%s", spec.Implementation, build, failure)
		}
	}
	if executable := spec.RestartCommand[0]; strings.ContainsRune(executable, filepath.Separator) {
		if !filepath.IsAbs(executable) {
			executable = filepath.Join(root, executable)
		}
		_, err := os.Stat(executable)
		require.NoError(t, err, "%s restart_command executable does not exist; build it first or set build_command", spec.Implementation)
	}
	return spec
}
