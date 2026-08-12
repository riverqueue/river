// Command syncrustmigrations mirrors River's canonical PostgreSQL migrations
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

const (
	canonicalDir = "riverdriver/riverpgxv5/migration/main"
	manifestPath = "conformance/migrations.json"
	mirrorDir    = "rust/riverqueue-migrate/migrations/main"
)

type manifest struct {
	Files []manifestFile `json:"files"`
	Line  string         `json:"line"`
}

type manifestFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func main() {
	check := flag.Bool("check", false, "check generated files without writing")
	flag.Parse()

	entries, err := os.ReadDir(canonicalDir)
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

	generatedManifest := manifest{Line: "main"}
	for _, name := range names {
		sourcePath := filepath.Join(canonicalDir, name)
		contents, err := os.ReadFile(sourcePath)
		if err != nil {
			fatal(err)
		}
		hash := sha256.Sum256(contents)
		generatedManifest.Files = append(generatedManifest.Files, manifestFile{
			Path:   filepath.ToSlash(sourcePath),
			SHA256: hex.EncodeToString(hash[:]),
		})

		mirrorPath := filepath.Join(mirrorDir, name)
		if *check {
			checkFile(mirrorPath, contents)
		} else {
			writeFile(mirrorPath, contents)
		}
	}

	manifestContents, err := json.MarshalIndent(&generatedManifest, "", "  ")
	if err != nil {
		fatal(err)
	}
	manifestContents = append(manifestContents, '\n')
	if *check {
		checkFile(manifestPath, manifestContents)
	} else {
		writeFile(manifestPath, manifestContents)
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
