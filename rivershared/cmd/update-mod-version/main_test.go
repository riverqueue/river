package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

const sampleGoMod = `module github.com/riverqueue/river

go 1.21

toolchain go1.22.5

require (
	github.com/riverqueue/river/riverdriver v0.0.0-00010101000000-000000000000
	github.com/riverqueue/river/riverdriver/riverdatabasesql v0.0.0-00010101000000-000000000000
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.0.12
)`

func TestParseAndUpdateGoModFile(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (string, *testBundle) {
		t.Helper()

		file, err := os.CreateTemp("", "go.mod")
		require.NoError(t, err)
		t.Cleanup(func() { os.Remove(file.Name()) })

		_, err = file.WriteString(sampleGoMod)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		return file.Name(), &testBundle{}
	}

	t.Run("WritesChanges", func(t *testing.T) {
		t.Parallel()

		filename, _ := setup(t)

		anyChanges, err := parseAndUpdateGoModFile(filename, "v0.0.13")
		require.NoError(t, err)
		require.True(t, anyChanges)

		// Reread the file that the command above just wrote and make sure the right
		// changes were made.
		fileData, err := os.ReadFile(filename)
		require.NoError(t, err)

		modFile, err := modfile.Parse(filename, fileData, nil)
		require.NoError(t, err)

		versions := make([]module.Version, 0, len(modFile.Require))
		for _, require := range modFile.Require {
			if require.Indirect || !strings.HasPrefix(require.Mod.Path, "github.com/riverqueue/river") {
				continue
			}

			versions = append(versions, require.Mod)
		}

		require.Equal(t, []module.Version{
			{Path: "github.com/riverqueue/river/riverdriver", Version: "v0.0.13"},
			{Path: "github.com/riverqueue/river/riverdriver/riverdatabasesql", Version: "v0.0.13"},
			{Path: "github.com/riverqueue/river/riverdriver/riverpgxv5", Version: "v0.0.13"},
		}, versions)

		// Running again is allowed and should be idempontent. This time it'll
		// return that no changes were made.
		anyChanges, err = parseAndUpdateGoModFile(filename, "v0.0.13")
		require.NoError(t, err)
		require.False(t, anyChanges)
	})
}
