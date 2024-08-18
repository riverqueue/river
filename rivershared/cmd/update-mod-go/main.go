// update-mod-go provides a command to help bump the `go/`toolchain` directives
// in the `go.mod`s of River's internal dependencies across the project. It's
// used to check that all directives match in CI, and to give us an easy way of
// updating them all at once during upgrades.
//
// To check that directives match, run with `CHECK`:
//
//	CHECK=true make update-mod-go
//
// To upgrade a `go`/`toolchain` directive, change it in the workspace's
// `go.work`, then run the program:
//
//	make update-mod-go
package main

import (
	"errors"
	"fmt"
	"os"
	"path"

	"golang.org/x/mod/modfile"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "failure: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	checkOnly := os.Getenv("CHECK") == "true"

	if len(os.Args) != 2 {
		return errors.New("expected exactly one arg, which should be the path to a go.work file")
	}

	workFilename := os.Args[1]

	workFileData, err := os.ReadFile(workFilename)
	if err != nil {
		return fmt.Errorf("error reading file %q: %w", workFilename, err)
	}

	workFile, err := modfile.ParseWork(workFilename, workFileData, nil)
	if err != nil {
		return fmt.Errorf("error parsing file %q: %w", workFilename, err)
	}

	var (
		workGoVersion     = workFile.Go.Version
		workToolchainName = workFile.Toolchain.Name
	)

	for _, workUse := range workFile.Use {
		if _, err := parseAndUpdateGoModFile(checkOnly, "./"+path.Join(workUse.Path, "go.mod"), workFilename, workGoVersion, workToolchainName); err != nil {
			return err
		}
	}

	if checkOnly {
		fmt.Printf("go/toolchain directives in all go.mod files match workspace\n")
	}

	return nil
}

func parseAndUpdateGoModFile(checkOnly bool, modFilename, workFilename, workGoVersion, workToolchainName string) (bool, error) {
	modFileData, err := os.ReadFile(modFilename)
	if err != nil {
		return false, fmt.Errorf("error reading file %q: %w", modFilename, err)
	}

	modFile, err := modfile.Parse(modFilename, modFileData, nil)
	if err != nil {
		return false, fmt.Errorf("error parsing file %q: %w", modFilename, err)
	}

	var anyMismatch bool

	fmt.Printf("%s\n", modFilename)

	if workGoVersion != modFile.Go.Version {
		if checkOnly {
			return false, fmt.Errorf("go directive of %q (%s) doesn't match %q (%s)", modFilename, modFile.Go.Version, workFilename, workGoVersion)
		}

		anyMismatch = true
		if err := modFile.AddGoStmt(workGoVersion); err != nil {
			return false, fmt.Errorf("error adding go statement: %w", err)
		}
		fmt.Printf("    set go to %s for %s\n", workGoVersion, modFilename)
	}

	if workToolchainName != modFile.Toolchain.Name {
		if checkOnly {
			return false, fmt.Errorf("toolchain directive of %q (%s) doesn't match %q (%s)", modFilename, modFile.Toolchain.Name, workFilename, workToolchainName)
		}

		anyMismatch = true
		if err := modFile.AddToolchainStmt(workToolchainName); err != nil {
			return false, fmt.Errorf("error adding toolchain statement: %w", err)
		}
		fmt.Printf("    set toolchain to %s for %s\n", workToolchainName, modFilename)
	}

	if !checkOnly {
		if anyMismatch {
			updatedFileData, err := modFile.Format()
			if err != nil {
				return false, fmt.Errorf("error formatting file %q after update: %w", modFilename, err)
			}

			if err := os.WriteFile(modFilename, updatedFileData, 0o600); err != nil {
				return false, fmt.Errorf("error writing file %q after update: %w", modFilename, err)
			}
		} else {
			fmt.Printf("    no changes\n")
		}
	}

	return anyMismatch, nil
}
