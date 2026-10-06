package harness

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImplementation(t *testing.T) {
	t.Parallel()

	t.Run("CommandBuildsOnce", func(t *testing.T) {
		t.Parallel()

		var (
			adapterDir  = t.TempDir()
			buildDir    = t.TempDir()
			builds      int
			gotBuildDir string
		)
		implementation := &Implementation{
			Build: func(buildDir string) (string, []string, error) {
				builds++
				gotBuildDir = buildDir
				return adapterDir, []string{"adapter", "--flag"}, nil
			},
			Name:     "custom",
			buildDir: buildDir,
		}

		for range 2 {
			command, err := implementation.command()
			require.NoError(t, err)
			require.Equal(t, []string{"adapter", "--flag"}, command)
		}
		require.Equal(t, 1, builds)
		require.Equal(t, buildDir, gotBuildDir)
		require.Equal(t, adapterDir, implementation.dir)
	})

	t.Run("CommandReturnsBuildError", func(t *testing.T) {
		t.Parallel()

		buildErr := errors.New("build failed")
		implementation := &Implementation{
			Build: func(buildDir string) (string, []string, error) {
				return "", nil, buildErr
			},
			Name: "custom",
		}

		_, err := implementation.command()
		require.ErrorIs(t, err, buildErr)
	})
}

func TestRunBuild(t *testing.T) {
	t.Parallel()

	t.Run("FailureIncludesOutput", func(t *testing.T) {
		t.Parallel()

		err := RunBuild(t.TempDir(), "go", "not-a-go-command")
		require.ErrorContains(t, err, "not-a-go-command")
		require.ErrorContains(t, err, "unknown command")
	})

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, RunBuild(t.TempDir(), "go", "version"))
	})
}

// TestUseImplementations replaces the package's known implementations, so it
// runs before the parallel scenarios that look them up, and restores them
// when it's done.
func TestUseImplementations(t *testing.T) { //nolint:paralleltest // replaces package state that parallel tests read
	original := knownImplementations
	t.Cleanup(func() { knownImplementations = original })

	first := &Implementation{Name: "first"}
	second := &Implementation{Name: "second"}
	UseImplementations(second, first)

	implementation, err := lookupImplementation("first")
	require.NoError(t, err)
	require.Same(t, first, implementation)

	implementation, err = lookupImplementation("second")
	require.NoError(t, err)
	require.Same(t, second, implementation)

	_, err = lookupImplementation("go")
	require.ErrorIs(t, err, errUnknownImplementation)
	require.ErrorContains(t, err, `"go" (known: first, second)`)
}
