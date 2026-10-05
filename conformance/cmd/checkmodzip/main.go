// Command checkmodzip fails if the module zip of any module in a Go workspace
// would include test fixtures or another language's port. It selects files
// with the same rules the Go module proxy uses, so it catches fixtures that a
// missing nested go.mod would otherwise publish with River. It reads the
// working tree, ignored files included, so it's exact on a clean checkout
// like CI's.
//
// Run it with a make target:
//
//	make check/modzip
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/zip"
)

// conformanceModulePath is this command's own module. It holds the shared
// fixtures by design and is never published, so it isn't checked.
const conformanceModulePath = "github.com/riverqueue/river/conformance"

// disallowedPathPattern matches paths, relative to a module's root, that must
// never be published: fixture and testdata directories, JSON files, and the
// conformance, JavaScript, and Rust trees.
var disallowedPathPattern = regexp.MustCompile(`(^|/)(fixtures?|testdata)/|\.json$|^(conformance|js|rust)/`)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: checkmodzip <path to go.work>")
		os.Exit(2)
	}

	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(workFilename string) error {
	workFileData, err := os.ReadFile(workFilename) //nolint:gosec // a developer-supplied go.work path
	if err != nil {
		return fmt.Errorf("error reading %s: %w", workFilename, err)
	}

	workFile, err := modfile.ParseWork(workFilename, workFileData, nil)
	if err != nil {
		return fmt.Errorf("error parsing %s: %w", workFilename, err)
	}

	var violations []error
	for _, use := range workFile.Use {
		dir := filepath.Join(filepath.Dir(workFilename), use.Path)

		modFilename := filepath.Join(dir, "go.mod")
		modFileData, err := os.ReadFile(modFilename) //nolint:gosec // a module directory listed in go.work
		if err != nil {
			return fmt.Errorf("error reading %s: %w", modFilename, err)
		}
		modulePath := modfile.ModulePath(modFileData)
		if modulePath == conformanceModulePath {
			continue
		}

		files, err := zip.CheckDir(dir)
		if err != nil {
			return fmt.Errorf("error checking module zip for %s: %w", modulePath, err)
		}

		for _, path := range files.Valid {
			relPath, err := filepath.Rel(dir, path)
			if err != nil {
				return fmt.Errorf("error making %s relative to %s: %w", path, dir, err)
			}
			relPath = filepath.ToSlash(relPath)

			if disallowedPathPattern.MatchString(relPath) {
				violations = append(violations, fmt.Errorf("%s: module zip would include %s", modulePath, relPath))
			}
		}

		fmt.Printf("%s: %d files\n", modulePath, len(files.Valid))
	}

	return errors.Join(violations...)
}
