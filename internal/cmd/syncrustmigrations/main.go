// Command syncrustmigrations mirrors River's canonical database migrations
// into the publishable Rust migration crate and records their hashes for
// cross-language conformance.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type database struct {
	canonicalDir string
	manifestPath string
	mirrorDir    string
	name         string
}

type manifest struct {
	Database string         `json:"database"`
	Files    []manifestFile `json:"files"`
	Line     string         `json:"line"`
}

type manifestFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func main() {
	check := flag.Bool("check", false, "check generated files without writing")
	flag.Parse()

	databases := []database{
		{
			canonicalDir: "riverdriver/riverpgxv5/migration/main",
			manifestPath: "conformance/migrations.json",
			mirrorDir:    "rust/riverqueue-migrate/migrations/main",
			name:         "postgres",
		},
		{
			canonicalDir: "riverdriver/riversqlite/migration/main",
			manifestPath: "conformance/migrations-sqlite.json",
			mirrorDir:    "rust/riverqueue-migrate/migrations/sqlite/main",
			name:         "sqlite",
		},
	}
	for _, database := range databases {
		syncDatabase(database, *check)
	}
}

func syncDatabase(database database, check bool) {
	entries, err := os.ReadDir(database.canonicalDir)
	if err != nil {
		fatal(err)
	}

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	slices.Sort(names)

	generatedManifest := manifest{Database: database.name, Line: "main"}
	for _, name := range names {
		sourcePath := filepath.Join(database.canonicalDir, name)
		contents, err := os.ReadFile(sourcePath)
		if err != nil {
			fatal(err)
		}
		hash := sha256.Sum256(contents)
		generatedManifest.Files = append(generatedManifest.Files, manifestFile{
			Path:   filepath.ToSlash(sourcePath),
			SHA256: hex.EncodeToString(hash[:]),
		})

		mirrorPath := filepath.Join(database.mirrorDir, name)
		if check {
			checkFile(mirrorPath, contents)
		} else {
			writeFile(mirrorPath, contents)
		}
	}
	removeStaleMirrors(database.mirrorDir, names, check)

	manifestContents, err := json.MarshalIndent(&generatedManifest, "", "  ")
	if err != nil {
		fatal(err)
	}
	manifestContents = append(manifestContents, '\n')
	if check {
		checkFile(database.manifestPath, manifestContents)
	} else {
		writeFile(database.manifestPath, manifestContents)
	}
}

func removeStaleMirrors(directory string, expected []string, check bool) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || slices.Contains(expected, entry.Name()) {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		if check {
			fatal(fmt.Errorf("generated file is stale: %s (run make generate/rust-migrations)", path))
		}
		if err := os.Remove(path); err != nil {
			fatal(err)
		}
	}
}

func checkFile(path string, expected []byte) {
	actual, err := os.ReadFile(path)
	if err != nil {
		fatal(fmt.Errorf("read generated file %s: %w", path, err))
	}
	if !bytes.Equal(actual, expected) {
		fatal(fmt.Errorf("generated file is stale: %s (run make generate/rust-migrations)", path))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func writeFile(path string, contents []byte) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatal(err)
	}
	//nolint:gosec // Generated repository artifacts are intentionally world-readable.
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		fatal(err)
	}
}
