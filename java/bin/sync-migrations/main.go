// Command sync-migrations mirrors River's canonical database migrations
// and generates the Java migration catalog. Run it from the repository root
// with `make generate/java-migrations` or `make verify/java-migrations`.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

func main() {
	check := flag.Bool("check", false, "check generated files without writing")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: sync-migrations [-check]")
		os.Exit(2)
	}
	if err := run(".", *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root string, check bool) error {
	destination := filepath.Join(root, "java/river/src/main/resources/com/riverqueue/migration")
	expected := make(map[string][]byte)
	var names []string
	for _, database := range []struct {
		dialect string
		driver  string
	}{
		{"postgres", "riverpgxv5"},
		{"sqlite", "riversqlite"},
	} {
		source := filepath.Join(root, "riverdriver", database.driver, "migration/main")
		paths, err := filepath.Glob(filepath.Join(source, "*.up.sql"))
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			return fmt.Errorf("no canonical migrations in %s", source)
		}
		current := make([]string, 0, len(paths))
		for i, path := range paths {
			name := strings.TrimSuffix(filepath.Base(path), ".up.sql")
			prefix, _, _ := strings.Cut(name, "_")
			version, err := strconv.Atoi(prefix)
			if err != nil || version != i+1 {
				return fmt.Errorf("migration versions must be consecutive in %s: %s", source, name)
			}
			current = append(current, name)
			for _, direction := range []string{"up", "down"} {
				filename := name + "." + direction + ".sql"
				contents, err := os.ReadFile(filepath.Join(source, filename))
				if err != nil {
					return fmt.Errorf("read canonical migration: %w", err)
				}
				expected[filepath.Join(destination, database.dialect, filename)] = contents
			}
		}
		if names != nil && !slices.Equal(current, names) {
			return errors.New("migration names differ between Postgres and SQLite; review the Java catalog")
		}
		names = current
	}
	expected[filepath.Join(destination, "index.txt")] = []byte(strings.Join(names, "\n") + "\n")

	var stale, extra []string
	for _, path := range slices.Sorted(maps.Keys(expected)) {
		actual, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("read migration mirror: %w", err)
		}
		if err != nil || !bytes.Equal(actual, expected[path]) {
			stale = append(stale, path)
		}
	}
	if err := filepath.WalkDir(destination, func(path string, entry fs.DirEntry, err error) error {
		if path == destination && errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".sql") {
			if _, ok := expected[path]; !ok {
				extra = append(extra, path)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan migration mirror: %w", err)
	}

	if check {
		if len(stale)+len(extra) > 0 {
			return fmt.Errorf("migration mirror differs; run `make generate/java-migrations`:\n%s", strings.Join(append(stale, extra...), "\n"))
		}
		fmt.Printf("Java migrations: %d versions per backend, verified\n", len(names))
		return nil
	}
	for _, path := range stale {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create migration directory: %w", err)
		}
		if err := os.WriteFile(path, expected[path], 0o644); err != nil { //nolint:gosec // Generated repository artifacts are intentionally world-readable.
			return fmt.Errorf("write migration mirror: %w", err)
		}
	}
	for _, path := range extra {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale migration: %w", err)
		}
	}
	fmt.Printf("Java migrations: %d versions per backend, synced\n", len(names))
	return nil
}
