// Package main provides a command to help bump the versions of River's internal
// dependencies in the `go.mod` files of submodules across the project. It's
// used to make the release process less error prone and less painful.
package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/semver"
)

// Notably, `./cmd/river` is excluded from this list. Unlike the other modules,
// it doesn't use `replace` directives so that it can stay installable with `go
// install ...@latest`. Without `replace`, dependencies need a hard lock in
// `go.sum`, so any changes to its `go.mod` file would require a `go mod tidy`
// be run afterwards.
var allProjectModules = []string{ //nolint:gochecknoglobals
	".",
	"./riverdriver",
	"./riverdriver/riverdatabasesql",
	"./riverdriver/riverpgxv5",
	"./rivershared",
	"./rivertype",
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "failure: %s", err)
		os.Exit(1)
	}
}

func run() error {
	version := os.Getenv("VERSION")
	if version == "" {
		return errors.New("expected to find VERSION in env")
	}

	if !semver.IsValid(version) {
		return fmt.Errorf("invalid semver version: %s", version)
	}

	for _, dir := range allProjectModules {
		if _, err := parseAndUpdateGoModFile(path.Join(dir, "go.mod"), version); err != nil {
			return err
		}
	}
	return nil
}

func parseAndUpdateGoModFile(filename, version string) (bool, error) {
	fileData, err := os.ReadFile(filename)
	if err != nil {
		return false, fmt.Errorf("error reading file %q: %w", filename, err)
	}

	modFile, err := modfile.Parse(filename, fileData, nil)
	if err != nil {
		return false, fmt.Errorf("error parsing file %q: %w", filename, err)
	}

	var anyChanges bool

	fmt.Printf("%s\n", filename)

	for _, require := range modFile.Require {
		if require.Indirect || !strings.HasPrefix(require.Mod.Path, "github.com/riverqueue/river") {
			continue
		}

		if require.Mod.Version == version {
			continue
		}

		anyChanges = true
		requirePath := require.Mod.Path

		// Not obvious from the name, but AddRequire replaces an existing
		// require statement if it exists, preserving any comments on it.
		if err := modFile.AddRequire(requirePath, version); err != nil {
			return false, fmt.Errorf("error adding require %q: %w", require.Mod.Path, err)
		}

		fmt.Printf("    set version to %s for %s\n", version, requirePath)
	}

	if anyChanges {
		updatedFileData, err := modFile.Format()
		if err != nil {
			return false, fmt.Errorf("error formatting file %q after update: %w", filename, err)
		}

		if err := os.WriteFile(filename, updatedFileData, 0o600); err != nil {
			return false, fmt.Errorf("error writing file %q after update: %w", filename, err)
		}
	} else {
		fmt.Printf("    no changes\n")
	}

	return anyChanges, nil
}
