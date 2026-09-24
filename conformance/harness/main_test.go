//go:build riverconformance

package harness_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// referenceBuild holds the Go reference adapter binary, built once per test
// process. Running the binary directly rather than through `go run` lets
// chaos scenarios kill the adapter process itself instead of the go tool.
var referenceBuild struct { //nolint:gochecknoglobals // one build per process
	directory string
	err       error
	once      sync.Once
}

func TestMain(m *testing.M) {
	code := m.Run()
	if referenceBuild.directory != "" {
		_ = os.RemoveAll(referenceBuild.directory)
	}
	// `go test -run` exits successfully when a pattern matches nothing. A
	// required CI run must execute at least one conformance test.
	if code == 0 && conformanceRequired() && conformanceTestsStarted.Load() == 0 {
		fmt.Fprintln(os.Stderr, "RIVER_CONFORMANCE_REQUIRED=1: no conformance test ran; check the -run pattern")
		code = 1
	}
	os.Exit(code)
}

// referenceAdapterCommand returns the command that starts the Go reference
// adapter, building it on first use.
func referenceAdapterCommand(t *testing.T, root string) []string {
	t.Helper()

	referenceBuild.once.Do(func() {
		// The binary outlives any single test, so TestMain removes it.
		directory, err := os.MkdirTemp("", "river-conformance-reference-") //nolint:usetesting // shared by every test in the process
		if err != nil {
			referenceBuild.err = err
			return
		}
		referenceBuild.directory = directory
		//nolint:gosec // Fixed arguments; only the temporary output path varies.
		command := exec.CommandContext(context.Background(), "go", "build", "-o", filepath.Join(directory, "riverconformanceadapter"), "./internal/cmd/riverconformanceadapter")
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			referenceBuild.err = fmt.Errorf("build Go reference adapter: %w\n%s", err, output)
		}
	})
	require.NoError(t, referenceBuild.err)
	return []string{filepath.Join(referenceBuild.directory, "riverconformanceadapter")}
}
