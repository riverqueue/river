package main

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckArchive(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		extra        string
		missingClass bool
		resource     string
		wantError    string
	}{
		{name: "Conformance", extra: "com/riverqueue/conformance/Adapter.class", wantError: "development content"},
		{name: "Fixture", extra: "fixtures/golden.json", wantError: "development content"},
		{name: "InnerTestClass", extra: "com/riverqueue/ClientTest$Helper.class", wantError: "development content"},
		{name: "JSON", extra: "com/riverqueue/protocol.json", wantError: "development content"},
		{name: "MissingClient", missingClass: true, resource: "SELECT 1;", wantError: "missing Client class"},
		{name: "MissingResource", wantError: "missing runtime resource"},
		{name: "StaleResource", resource: "SELECT 2;", wantError: "stale runtime resource"},
		{name: "TestSource", extra: "com/riverqueue/ClientTests.java", wantError: "development content"},
		{name: "Valid", resource: "SELECT 1;"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			resources := filepath.Join(root, "resources")
			require.NoError(t, os.MkdirAll(filepath.Join(resources, "com/riverqueue"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(resources, "com/riverqueue/postgres.sql"), []byte("SELECT 1;"), 0o600))
			path := filepath.Join(root, "river.jar")
			file, err := os.Create(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })
			archive := zip.NewWriter(file)
			contents := map[string]string{}
			if !testCase.missingClass {
				contents["com/riverqueue/Client.class"] = "bytecode"
			}
			if testCase.resource != "" {
				contents["com/riverqueue/postgres.sql"] = testCase.resource
			}
			if testCase.extra != "" {
				contents[testCase.extra] = "development content"
			}
			for name, content := range contents {
				writer, err := archive.Create(name)
				require.NoError(t, err)
				_, err = writer.Write([]byte(content))
				require.NoError(t, err)
			}
			require.NoError(t, archive.Close())
			require.NoError(t, file.Close())

			err = checkArchive(path, resources)
			if testCase.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantError)
			}
		})
	}
}
