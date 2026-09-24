package harness_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// adapterSpec is a candidate descriptor (see candidate.schema.json) after
// environment expansion.
type adapterSpec struct {
	ApplicationName     string                      `json:"application_name"`
	BuildCommand        []string                    `json:"build_command"`
	Command             []string                    `json:"command"`
	Implementation      string                      `json:"implementation"`
	Performance         map[string]performanceBound `json:"performance"`
	Profiles            []string                    `json:"profiles"`
	ReleaseBuildCommand []string                    `json:"release_build_command"`
	ReleaseCommand      []string                    `json:"release_command"`
	RestartCommand      []string                    `json:"restart_command"`
	StartOptions        []string                    `json:"start_options"`
	Version             string                      `json:"version"`
}

// performanceBound is a candidate's declared release performance bound
// relative to the reference implementation for one benchmark mode.
type performanceBound struct {
	MaxP95Ratio        float64 `json:"max_p95_ratio"`
	MinThroughputRatio float64 `json:"min_throughput_ratio"`
}

// defaultPerformanceBounds apply to modes a descriptor does not declare.
var defaultPerformanceBounds = map[string]performanceBound{ //nolint:gochecknoglobals // descriptor default
	// Enqueue uses equivalent ordinary insertion mechanisms but remains
	// driver/runtime-language sensitive. It is a regression guard, not an
	// incentive to add a candidate-only fast producer path.
	"enqueue": {MaxP95Ratio: 2, MinThroughputRatio: 0.4},
	"mixed":   {MaxP95Ratio: 1.25, MinThroughputRatio: 0.8},
	"worker":  {MaxP95Ratio: 1.25, MinThroughputRatio: 0.8},
}

func decodeDescriptor(t *testing.T, document []byte) adapterSpec {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(document))
	var raw map[string]json.RawMessage
	require.NoError(t, decoder.Decode(&raw))
	delete(raw, "$schema")
	stripped, err := json.Marshal(raw)
	require.NoError(t, err)
	decoder = json.NewDecoder(bytes.NewReader(stripped))
	decoder.DisallowUnknownFields()
	var spec adapterSpec
	require.NoError(t, decoder.Decode(&spec), "candidate descriptor has an unknown or invalid field")
	require.NotEmpty(t, spec.ApplicationName)
	require.True(t, strings.HasPrefix(spec.ApplicationName, "river-conformance-"),
		"candidate application_name %q must start with river-conformance- so fault injection can target it", spec.ApplicationName)
	require.NotEmpty(t, spec.Command)
	require.NotEmpty(t, spec.Implementation)
	for _, command := range []*[]string{
		&spec.BuildCommand, &spec.Command, &spec.ReleaseBuildCommand, &spec.ReleaseCommand, &spec.RestartCommand,
	} {
		*command = expandDescriptorCommand(*command)
	}
	for mode, bound := range spec.Performance {
		require.Contains(t, defaultPerformanceBounds, mode, "unknown performance mode %q", mode)
		require.Positive(t, bound.MaxP95Ratio, "performance.%s.max_p95_ratio", mode)
		require.Positive(t, bound.MinThroughputRatio, "performance.%s.min_throughput_ratio", mode)
	}
	return spec
}

// expandDescriptorCommand expands `${NAME}` and `${NAME:-default}` in each
// argument, so a descriptor can reference a build output directory such as
// CARGO_TARGET_DIR without hardcoding it.
func expandDescriptorCommand(command []string) []string {
	if command == nil {
		return nil
	}
	expanded := make([]string, len(command))
	for index, argument := range command {
		expanded[index] = os.Expand(argument, func(reference string) string {
			name, fallback, hasFallback := strings.Cut(reference, ":-")
			if value := os.Getenv(name); value != "" || !hasFallback {
				return value
			}
			return fallback
		})
	}
	return expanded
}
