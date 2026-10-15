package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
)

// Implementation is a River implementation the harness can run an adapter
// for. Each is built once per test process, on first use.
type Implementation struct {
	// Build builds the implementation's adapter, writing anything it builds
	// under buildDir, a directory the harness removes when the test process
	// ends. It returns the directory the adapter runs in and the command that
	// starts it.
	Build func(buildDir string) (dir string, command []string, err error)

	// Name is the name the RIVER_CONFORMANCE variables select the
	// implementation by, such as "go", "rust", or "js".
	Name string

	// Performance bounds the implementation's benchmarks relative to the
	// reference's, by mode.
	Performance map[string]PerformanceBound

	buildDir string
	cmd      []string
	dir      string
	err      error
	once     sync.Once
}

// command builds the implementation's adapter if necessary and returns the
// command that starts it.
func (i *Implementation) command() ([]string, error) {
	i.once.Do(func() {
		i.dir, i.cmd, i.err = i.Build(i.buildDir)
	})
	return i.cmd, i.err
}

// UseImplementations replaces the implementations the harness knows with
// implementations, which RIVER_CONFORMANCE, RIVER_CONFORMANCE_REFERENCE, and
// RIVER_CONFORMANCE_PEER then select by name. It lets another module run its
// own scenarios against adapters of its own. Call it from TestMain before
// Main, since Main assigns each implementation its build directory.
func UseImplementations(implementations ...*Implementation) {
	knownImplementations = make(map[string]*Implementation, len(implementations))
	for _, implementation := range implementations {
		knownImplementations[implementation.Name] = implementation
	}
}

// riverBuild returns a build function for one of River's own adapters, which
// builds from the River repository's root.
func riverBuild(build func(root, buildDir string) ([]string, error)) func(buildDir string) (string, []string, error) {
	return func(buildDir string) (string, []string, error) {
		root, err := repoRoot()
		if err != nil {
			return "", nil, err
		}
		command, err := build(root, buildDir)
		return root, command, err
	}
}

// knownImplementations are those the harness knows, by name: River's own
// unless UseImplementations replaced them.
var knownImplementations = map[string]*Implementation{ //nolint:gochecknoglobals // built once per test process
	"go": {
		Name:        "go",
		Performance: defaultPerformance,
		Build: riverBuild(func(root, buildDir string) ([]string, error) {
			// The binary runs directly rather than through `go run`, so a
			// killed adapter is the adapter itself.
			binary := filepath.Join(buildDir, "riverconformanceadapter-go")
			if err := RunBuild(filepath.Join(root, "conformance"), "go", "build", "-o", binary, "./cmd/riverconformanceadapter"); err != nil {
				return nil, err
			}
			return []string{binary}, nil
		}),
	},
	"js": {
		Name: "js",
		Performance: map[string]PerformanceBound{
			"enqueue": {MaxP95Ratio: 3, MinThroughputRatio: 0.25},
			"mixed":   {MaxP95Ratio: 2, MinThroughputRatio: 0.5},
			"worker":  {MaxP95Ratio: 2, MinThroughputRatio: 0.5},
		},
		Build: riverBuild(func(root, buildDir string) ([]string, error) {
			// The adapter's workspace dependencies run from their builds,
			// which `...` includes, except the root riverqueue package.
			jsRoot := filepath.Join(root, "js")
			if err := RunBuild(jsRoot, "pnpm", "run", "build"); err != nil {
				return nil, err
			}
			if err := RunBuild(jsRoot, "pnpm", "--filter", "@riverqueue/conformance...", "run", "build"); err != nil {
				return nil, err
			}
			return []string{"node", filepath.Join(root, "js", "conformance", "dist", "main.js")}, nil
		}),
	},
	"rust": {
		Name:        "rust",
		Performance: defaultPerformance,
		Build: riverBuild(func(root, buildDir string) ([]string, error) {
			workspace := filepath.Join(root, "rust")
			if err := RunBuild(workspace, "cargo", "build", "--locked", "-p", "riverqueue-conformance"); err != nil {
				return nil, err
			}
			// The binary runs directly rather than through `cargo run`, so a
			// killed adapter is the adapter itself. Cargo resolves a relative
			// CARGO_TARGET_DIR against the directory it runs in.
			targetDir := cmpOr(os.Getenv("CARGO_TARGET_DIR"), "target")
			if !filepath.IsAbs(targetDir) {
				targetDir = filepath.Join(workspace, targetDir)
			}
			return []string{filepath.Join(targetDir, "debug", "riverqueue-conformance")}, nil
		}),
	},
}

// PerformanceBound bounds a benchmark relative to the reference's: the
// lowest throughput ratio and the highest p95 latency ratio.
type PerformanceBound struct {
	MaxP95Ratio        float64
	MinThroughputRatio float64
}

// defaultPerformance bounds implementations that declare no bounds of their
// own. Enqueueing remains sensitive to driver and language, so it's only a
// regression guard.
var defaultPerformance = map[string]PerformanceBound{ //nolint:gochecknoglobals // constant
	"enqueue": {MaxP95Ratio: 2, MinThroughputRatio: 0.4},
	"mixed":   {MaxP95Ratio: 1.25, MinThroughputRatio: 0.8},
	"worker":  {MaxP95Ratio: 1.25, MinThroughputRatio: 0.8},
}

// lookupImplementation returns the implementation named name.
func lookupImplementation(name string) (*Implementation, error) {
	implementation, ok := knownImplementations[name]
	if !ok {
		names := slices.Sorted(maps.Keys(knownImplementations))
		return nil, fmt.Errorf("%w %q (known: %s)", errUnknownImplementation, name, strings.Join(names, ", "))
	}
	return implementation, nil
}

// RunBuild runs command in dir as a step of an implementation's Build. When
// the command fails, the error it returns includes the command's output.
func RunBuild(dir string, command ...string) error {
	cmd := exec.CommandContext(context.Background(), command[0], command[1:]...) //nolint:gosec // fixed build commands
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("error running %v: %w\n%s", command, err, output)
	}
	return nil
}

// repoRoot returns the root of the River repository.
func repoRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("error locating the harness source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.work")); err != nil {
		return "", fmt.Errorf("error finding the repository root at %s: %w", root, err)
	}
	return root, nil
}
