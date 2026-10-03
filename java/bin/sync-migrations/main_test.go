package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		destination string
		root        string
		sources     []string
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		root := t.TempDir()
		bundle := &testBundle{
			destination: filepath.Join(root, "java/river/src/main/resources/com/riverqueue/migration"),
			root:        root,
		}
		for _, driver := range []string{"riverpgxv5", "riversqlite"} {
			source := filepath.Join(root, "riverdriver", driver, "migration/main")
			require.NoError(t, os.MkdirAll(source, 0o755))
			for _, direction := range []string{"up", "down"} {
				require.NoError(t, os.WriteFile(filepath.Join(source, "001_first."+direction+".sql"), []byte(driver+" "+direction+"\n"), 0o600))
			}
			bundle.sources = append(bundle.sources, source)
		}
		return bundle
	}

	t.Run("CheckAndRepairDrift", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		require.ErrorContains(t, run(bundle.root, true), "migration mirror differs")
		require.NoDirExists(t, bundle.destination)
		require.NoError(t, run(bundle.root, false))
		require.NoError(t, run(bundle.root, true))
		index, err := os.ReadFile(filepath.Join(bundle.destination, "index.txt"))
		require.NoError(t, err)
		require.Equal(t, "001_first\n", string(index))

		missing := filepath.Join(bundle.destination, "postgres/001_first.down.sql")
		stale := filepath.Join(bundle.destination, "sqlite/001_first.up.sql")
		extra := filepath.Join(bundle.destination, "sqlite/999_extra.up.sql")
		require.NoError(t, os.Remove(missing))
		require.NoError(t, os.WriteFile(stale, []byte("stale"), 0o600))
		require.NoError(t, os.WriteFile(extra, []byte("extra"), 0o600))

		err = run(bundle.root, true)
		for _, path := range []string{missing, stale, extra} {
			require.ErrorContains(t, err, path)
		}
		require.NoFileExists(t, missing)
		contents, err := os.ReadFile(stale)
		require.NoError(t, err)
		require.Equal(t, "stale", string(contents))
		require.FileExists(t, extra)

		require.NoError(t, run(bundle.root, false))
		require.NoError(t, run(bundle.root, true))
		require.NoFileExists(t, extra)
		contents, err = os.ReadFile(stale)
		require.NoError(t, err)
		require.Equal(t, "riversqlite up\n", string(contents))
	})

	t.Run("InvalidSourcesDoNotWrite", func(t *testing.T) {
		t.Parallel()

		for _, testCase := range []struct {
			name      string
			rename    string
			wantError string
		}{
			{name: "DifferentNames", rename: "001_other", wantError: "migration names differ"},
			{name: "MissingDown", wantError: "read canonical migration"},
			{name: "Nonconsecutive", rename: "002_first", wantError: "must be consecutive"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				bundle := setup(t)
				source := bundle.sources[1]

				if testCase.rename == "" {
					require.NoError(t, os.Remove(filepath.Join(source, "001_first.down.sql")))
				} else {
					for _, direction := range []string{"up", "down"} {
						require.NoError(t, os.Rename(filepath.Join(source, "001_first."+direction+".sql"), filepath.Join(source, testCase.rename+"."+direction+".sql")))
					}
				}
				require.ErrorContains(t, run(bundle.root, false), testCase.wantError)
				require.NoDirExists(t, bundle.destination)
			})
		}
	})
}
